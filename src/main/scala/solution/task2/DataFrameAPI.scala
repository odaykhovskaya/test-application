package solution.task2

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameAPI {

  val spark = SparkSession.builder.getOrCreate()

  import spark.implicits._

  def getTopCampaigns(): DataFrame = {

    /*
    Step 0: getting dataframe from Task1
     */
    val df = solution.task1.DataFrameAPI.getUserSessions()

    /*
    Step 1: get Top Campaigns
     */
    val topCampaigns = df
      .filter($"isConfirmed" === lit("TRUE"))
      .groupBy("campaignId")
      .agg(sum($"billingCost").as("totalBillingCost"))
      .orderBy($"totalBillingCost".desc)
      .limit(10)

    topCampaigns

  }

  def getTopChannels(): DataFrame = {

    /*
    Step 0: getting dataframe from Task1
     */
    val df = solution.task1.DataFrameAPI.getUserSessions()

    /*
    Step 1: get Top Channels
     */
    val w = Window.partitionBy($"campaignId").orderBy($"uniqueSessions".desc)

    val topChannels = df
      .groupBy($"campaignId", $"channelId")
      .agg(countDistinct($"sessionId").as("uniqueSessions"))
      .withColumn("row_num", row_number().over(w))
      .filter($"row_num" === lit(1))
      .drop("row_num")

    topChannels

  }


}
