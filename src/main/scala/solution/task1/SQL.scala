package solution.task1

import org.apache.spark.sql.{DataFrame, SparkSession}
import solution.Config

object SQL {

  def run(): DataFrame = {

    val spark = SparkSession.builder.getOrCreate()

    /*
    Step 0: reading data and creating views
     */
    spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", "true")
      .load(Config.PATH_TO_DATA_CLICKSTREAM)
      .createTempView("clickstream")

    spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", "true")
      .load(Config.PATH_TO_DATA_PURCHASES)
      .createTempView("purchases")

    /*
    Step 1: formatting json in 'attributes column' and extracting purchaseId, campaignId and channelId. Formatting billingCost in purchases
     */
    spark.sql(
      """
        |SELECT userId, eventId, eventTime, eventType,
        |CASE WHEN eventType == 'purchase' THEN string(get_json_object(attributes, '$.purchase_id')) ELSE null END AS purchase_id,
        |CASE WHEN eventType == 'app_open' THEN string(get_json_object(attributes, '$.campaign_id')) ELSE null END AS campaignId,
        |CASE WHEN eventType == 'app_open' THEN string(get_json_object(attributes, '$.channel_id')) ELSE null END AS channelId
        |FROM (
        | SELECT userId, eventId, eventTime, eventType, regexp_replace(regexp_extract(attributes, '^\\{(.+)\\}$', 1), '“', '"') as attributes
        | FROM clickstream
        | )
        |""".stripMargin)
      .createOrReplaceTempView("clickstream")

    spark.sql(
      """
        |SELECT purchaseId, purchaseTime, regexp_replace(billingCost, ",", ".") AS billingCost, isConfirmed FROM purchases
        |""".stripMargin)
      .createOrReplaceTempView("purchases")

    /*
    Step 2: joining clickStream and purchases dataframes to get info about purchases
     */
    spark.sql(
      """
        |SELECT * FROM (
        | clickstream LEFT JOIN purchases ON purchase_id == purchaseId
        | )
        |""".stripMargin)
      .createOrReplaceTempView("clickstream_enriched")

    /*
    Step 3: getting session id for all events
     */

    spark.sql(
      """
        |SELECT *, concat_ws('_', userId, session_id_for_user) AS sessionId FROM
        | ( SELECT *, sum(session_counter) OVER (PARTITION BY userId ORDER BY eventTime) AS session_id_for_user FROM
        |    (
        |     SELECT *, CASE WHEN lag_event_id == 'app_close' THEN 1 ELSE 0 END AS session_counter
        |     FROM ( SELECT *, lag(eventType, 1) OVER (PARTITION BY userId ORDER BY eventTime) AS lag_event_id FROM clickstream_enriched )
        |    )
        |  )
        |""".stripMargin).createOrReplaceTempView("clickstream_sessions_1")

    /*
    Step 4: populating campaignId and channelId for all session's rows
     */

    spark.sql(
      """
        |SELECT purchaseId, purchaseTime, billingCost, isConfirmed, sessionId,
        | first(campaignId, true) OVER (PARTITION BY sessionId) AS campaignId,
        | first(channelId, true) OVER (PARTITION BY sessionId) AS channelId
        | FROM clickstream_sessions_1
        |""".stripMargin)
      .createOrReplaceTempView("clickstream_sessions_2")

    /*
    Step 5: filtering to get only sessions with purchases
     */
    spark.sql(
      """
        |SELECT purchaseId, purchaseTime, billingCost, isConfirmed, sessionId, campaignId, channelId
        |FROM clickstream_sessions_2
        |WHERE purchaseId IS NOT null
        |""".stripMargin)

  }

}
