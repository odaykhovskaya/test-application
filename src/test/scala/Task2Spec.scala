import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}

class Task2Spec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession
    .builder()
    .appName(s"UnitTest Job")
    .config("spark.master", "local")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()
  }

  "solution.task1.Task1" should "do smth" in {
    val result = solution.task2.SQL.getTopCampaigns()
    result.show
  }

  "solution.task1.Task2" should "do smth" in {
    val result = solution.task2.SQL.getTopChannels()
    result.show
  }

}
