package usd.apps

import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import usd.data.config.CcyEnv
import usd.data.config.ElementalTables
import usd.util.DateUtils
import usd.util.CcyLogging
import usd.data.source.CurrencyPairs
import usd.data.tables.FetchYahooPrices

case class ElementalConf(arguments: Seq[String]) 
    extends ScallopConf(arguments) {

  val envName = opt[String](required = true)
  val startDate = opt[String](required=true)
  val endDate = opt[String](required=true)
  val job = opt[String](required=true)

  verify()
  val fromDate = DateUtils(startDate()).dt
  val toDate = DateUtils(endDate()).dt
}

object ElementalJob extends CcyLogging {
  def main(args: Array[String]) = {
    val conf = new ElementalConf(args.toSeq)
    implicit val spark : SparkSession = SparkBase("elemental-job").spark
    implicit val env =
      new CcyEnv(conf.envName()) 

    conf.job() match {
      case "fetch-yahoo-prices" =>
        new FetchYahooPrices (
          stocks= Seq("SPY"),
          fxPairs = Seq(CurrencyPairs.AUDUSD))(spark, env)
          .processDates(conf.fromDate, conf.toDate)
          .show()
      case _ =>
        throw new Exception("not implemented")
    }
  }
}

