package solution

object Config {

  val PATH_TO_DATA_PURCHASES: String = scala.util.Properties.envOrElse("PATH_TO_DATA", "/Users/odaykhovskaya/Documents/01Work/TestApp/test-application/src/main//resources/sample/purchases_sample.xlsx")
  val PATH_TO_DATA_CLICKSTREAM: String = scala.util.Properties.envOrElse("PATH_TO_DATA_CLICKSTREAM", "/Users/odaykhovskaya/Documents/01Work/TestApp/test-application/src/main//resources/sample/mobile-app-clickstream_sample.xlsx")

}
