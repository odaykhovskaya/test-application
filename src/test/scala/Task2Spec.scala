import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}
import solution.Config

class Task2Spec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession
    .builder()
    .appName(s"UnitTest Job")
    .config("spark.master", "local")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()
  }

  val expectedResultSizeCampaign = 2
  val expectedBestCampaign = "cmp1"

  val expectedResultSizeChannel = 2
  val expectedCmp1Channel = "Google Ads"

  "solution.task2.DataFrameAPI Campaign Analysis" should "work correctly" in {
    val result = solution.task2.DataFrameAPI.getTopCampaigns()

    val actualResultSizeCampaign = result.count()
    assertResult(expectedResultSizeCampaign)(actualResultSizeCampaign)

    val actualBestCampaign = result.limit(1).collectAsList().get(0).getAs[String]("campaignId")
    assertResult(expectedBestCampaign)(actualBestCampaign)

  }

  "solution.task2.SQL Campaign Analysis" should "work correctly" in {
    val result = solution.task2.SQL.getTopCampaigns()

    val actualResultSizeCampaign = result.count()
    assertResult(expectedResultSizeCampaign)(actualResultSizeCampaign)

    val actualBestCampaign = result.limit(1).collectAsList().get(0).getAs[String]("campaignId")
    assertResult(expectedBestCampaign)(actualBestCampaign)

  }

  "solution.task2.DataFrameAPI" should "do smth" in {
    val result = solution.task2.DataFrameAPI.getTopChannels()

    val actualResultSizeChannel = result.count()
    assertResult(expectedResultSizeChannel)(actualResultSizeChannel)

    val actualCmp1Channel =
      result
        .filter(col("campaignId")===lit("cmp1"))
        .collectAsList()
        .get(0)
        .getAs[String]("channelId")
    assertResult(expectedCmp1Channel)(actualCmp1Channel)

  }

  "solution.task2.SQL" should "do smth" in {
    val result = solution.task2.SQL.getTopChannels()

    val actualResultSizeChannel = result.count()
    assertResult(expectedResultSizeChannel)(actualResultSizeChannel)

    val actualCmp1Channel =
      result
        .filter(col("campaignId")===lit("cmp1"))
        .collectAsList()
        .get(0)
        .getAs[String]("channelId")
    assertResult(expectedCmp1Channel)(actualCmp1Channel)

  }

  "Result file" should "be written" in {
    val resultCampaign = solution.task2.DataFrameAPI.getTopCampaigns()

    resultCampaign.write.format("com.crealytics.spark.excel")
      .option("useHeader", "true").save(s"${Config.PATH_TO_RESULT}/task2_campaigns.xlsx")

    val resultChannel = solution.task2.DataFrameAPI.getTopChannels()

    resultChannel.write.format("com.crealytics.spark.excel")
      .option("useHeader", "true")
      .mode("overwrite")
      .save(s"${Config.PATH_TO_RESULT}/task2_channels.xlsx")

  }

}
