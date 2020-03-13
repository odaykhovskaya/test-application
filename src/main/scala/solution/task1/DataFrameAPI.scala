package solution.task1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import solution.Config

object DataFrameAPI {

  val spark = SparkSession.builder.getOrCreate()

  import spark.implicits._

  def getUserSessions(): DataFrame = {
    /*
    Step 0: reading data
     */
    val clickStream = spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", "true")
      .load(Config.PATH_TO_DATA_CLICKSTREAM)

    var purchases = spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", "true")
      .load(Config.PATH_TO_DATA_PURCHASES)

    /*
        Step 1: formatting json in 'attributes column' and extracting purchaseId, campaignId and channelId. Formatting billingCost in purchases
         */
    val step1 = clickStream
      .select(
        $"userId", $"eventId", $"eventTime", $"eventType",
        regexp_extract($"attributes", "^\\{(.+)\\}$", 1).as("attributes")
      )
      .withColumn("attributes", regexp_replace($"attributes", "“", "\""))
      .withColumn(
        "c_purchaseId",
        when($"eventType" === lit("purchase"), get_json_object($"attributes", "$.purchase_id")).otherwise(null)
      )
      .withColumn(
        "campaignId",
        when($"eventType" === lit("app_open"), get_json_object($"attributes", "$.campaign_id")).otherwise(null)
      )
      .withColumn(
        "channelId",
        when($"eventType" === lit("app_open"), get_json_object($"attributes", "$.channel_id")).otherwise(null)
      )

    purchases = purchases
      .withColumn("billingCost", regexp_replace($"billingCost", ",", ".").cast(DoubleType))

    /*
    Step 2: joining clickStream and purchases dataframes to get info about purchases
     */
    val step2 = step1
      .join(purchases, $"c_purchaseId" === $"purchaseId", "left")

    /*
    Step 3: getting session id for all events
     */
    val wUserId = Window.partitionBy($"userId").orderBy($"eventTime".asc)

    val step3 = step2
      .withColumn("lag_event_id", lag($"eventType", 1).over(wUserId))
      .withColumn("session_counter", when($"lag_event_id" === lit("app_close"), 1).otherwise(0))
      .withColumn("session_id_for_user", sum($"session_counter").over(wUserId))
      .withColumn("sessionId", concat_ws("_", $"userId", $"session_id_for_user"))

    /*
    Step 4: populating campaignId and channelId for all session's rows
     */
    val wSessionId = Window.partitionBy($"sessionId")

    val step4 = step3
      .withColumn("campaignId", first($"campaignId", true).over(wSessionId))
      .withColumn("channelId", first($"channelId", true).over(wSessionId))

    step4

  }

  def run(): DataFrame = {

    val df = getUserSessions()

    /*
    Step 5: filtering to get only sessions with purchases
     */
    val result = df
    .filter($"purchaseId".isNotNull)
    .select($"purchaseId", $"purchaseTime", $"billingCost", $"isConfirmed", $"sessionId", $"campaignId", $"channelId")

    result

  }


}
