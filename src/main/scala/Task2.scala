import org.apache.spark.sql.SparkSession
import solution.Config

class Task2 {

  val spark = SparkSession.builder.getOrCreate()

  import spark.implicits._

  val clickStream = spark.read.format("com.crealytics.spark.excel")
    .load(Config.PATH_TO_DATA_CLICKSTREAM)

  val purchases = spark.read.format("com.crealytics.spark.excel")
    .load(Config.PATH_TO_DATA_CLICKSTREAM)

  spark.sql(
    """
      |select userId, eventId, eventTime, eventType, attributes
      |from clickstream
      |""".stripMargin).show

}
