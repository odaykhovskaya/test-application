import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}

class Task1Spec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession
    .builder()
    .appName(s"UnitTest Job")
    .config("spark.master", "local")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()
  }

  "solution.task1.DataFrameAPI" should "work correctly in" in {

    val result = solution.task1.DataFrameAPI.run()
    result.show
  }

  "solution.task1.SQL" should "work correctly in" in {

    val result = solution.task1.SQL.run()
    result.show
  }

}
