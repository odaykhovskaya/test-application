package solution.task2

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQL {

  val spark = SparkSession.builder.getOrCreate()

  import spark.implicits._

  def getTopCampaigns(): DataFrame = {

    /*
    Step 0: getting table's views from Task1
     */
    solution.task1.SQL.run()

    /*
    Step 1: get Top Campaigns
     */
    spark.sql(
      """
        |SELECT campaignId, totalBillingCost FROM
        | (
        |   SELECT campaignId, sum(billingCost) AS totalBillingCost FROM
        |     (
        |       SELECT * FROM clickstream_sessions_2 WHERE isConfirmed == 'TRUE'
        |     )
        |   GROUP BY campaignId
        |  )
        |ORDER BY totalBillingCost DESC LIMIT 10
        |""".stripMargin)

  }

  def getTopChannels(): DataFrame = {

    /*
    Step 0: getting table's views from Task1
     */
    solution.task1.SQL.run()

    /*
    Step 1: get Top Channels
     */
    spark.sql(
      """
        |SELECT campaignId, channelId, uniqueSessions FROM
        | (
        |   SELECT *, row_number() OVER (PARTITION BY campaignId ORDER BY uniqueSessions DESC) AS row_num FROM
        |   (
        |     SELECT campaignId, channelId, count(DISTINCT sessionId) AS uniqueSessions FROM clickstream_sessions_2 GROUP BY campaignId, channelId
        |   )
        | )
        |WHERE row_num == 1
        |""".stripMargin)


  }


}
