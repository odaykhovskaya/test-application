package solution.task2

object Main extends App {

  override def main(args: Array[String]): Unit = {

    DataFrameAPI.getTopCampaigns()

    DataFrameAPI.getTopChannels()

    SQL.getTopCampaigns()

    SQL.getTopChannels()

  }
}
