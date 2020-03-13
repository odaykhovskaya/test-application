import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
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

  val expectedResultSize = 6L
  val expectedBiggestPurchaseId = "p3"

  "solution.task1.DataFrameAPI" should "work correctly in" in {

    val result = solution.task1.DataFrameAPI.run()

    val actualResultSize = result.count()
    assertResult(expectedResultSize)(actualResultSize)

    val actualBiggestPurchaseId =
      result
      .orderBy(col("billingCost").desc)
      .limit(1)
      .collectAsList()
      .get(0).getAs[String]("purchaseId")
    assertResult(expectedBiggestPurchaseId)(actualBiggestPurchaseId)

  }

  "solution.task1.SQL" should "work correctly in" in {

    val result = solution.task1.SQL.run()

    val actualResultSize = result.count()
    assertResult(expectedResultSize)(actualResultSize)

    val actualBiggestPurchaseId =
      result
        .orderBy(col("billingCost").desc)
        .limit(1)
        .collectAsList()
        .get(0).getAs[String]("purchaseId")
    assertResult(expectedBiggestPurchaseId)(actualBiggestPurchaseId)

  }

}
