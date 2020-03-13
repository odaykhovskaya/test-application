package solution

object Config {

  val PATH_TO_DATA_PURCHASES: String = scala.util.Properties.envOrElse("PATH_TO_DATA_PURCHASES", "./src/main/resources/sample/purchases_sample.xlsx")
  val PATH_TO_DATA_CLICKSTREAM: String = scala.util.Properties.envOrElse("PATH_TO_DATA_CLICKSTREAM", "./src/main/resources/sample/mobile-app-clickstream_sample.xlsx")

  val PATH_TO_RESULT: String = scala.util.Properties.envOrElse("PATH_TO_RESULT", "./src/main/resources/result")

}
