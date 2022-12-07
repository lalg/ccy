package usd.apps

import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import usd.data.config.CcyEnv
import usd.data.config.ElementalTables
import usd.util.DateUtils
import usd.util.CcyLogging
import usd.data.source.CurrencyPairs
import usd.data.tables.FetchYahooPrices

// arguments:
//   --env-name en --job j --start-date sd --end-date ed
//   --env-name en --job j --backfillYear yy
case class ElementalConf(arguments: Seq[String]) 
    extends ScallopConf(arguments) {

  val envName = opt[String](required = true)
  val job = opt[String](required=true)
  val startDate = opt[String]()
  val endDate = opt[String]()
  val backfillYear = opt[Int]()
  val stocks = opt[List[String]]()
  val fx = opt[List[String]]()  

  verify()

  assert(
    (startDate.isDefined && endDate.isDefined) ||
      (startDate.isEmpty && endDate.isEmpty && backfillYear.isDefined),
    "bad option combination")

  def fromDate =
    if (startDate.isDefined) DateUtils(startDate()).dt
    else DateUtils(s"${backfillYear()}-01-01").dt

  def toDate =
    if (endDate.isDefined) DateUtils(endDate()).dt
    else DateUtils(s"${backfillYear() + 1}-01-01").dt

  def stockList = stocks.getOrElse(List.empty).toIndexedSeq
  def fxList =
    fx.getOrElse(List.empty)
      .map (CurrencyPairs.withName _)
      .toSeq
  def symbols = stockList ++ fxList
}

object ElementalJob extends CcyLogging {
  def main(args: Array[String])(implicit spark : SparkSession) = {
    val conf = new ElementalConf(args.toSeq)
    implicit val env =
      new CcyEnv(conf.envName()) 

    conf.job() match {
      case "fetch-yahoo-prices" =>
        new FetchYahooPrices (
          stocks= conf.stockList,
          fxPairs = conf.fxList)(spark, env)
          .processDatesAndSave(conf.fromDate, conf.toDate)

      case "debug" =>
        new FetchYahooPrices (
          stocks= conf.stockList,
          fxPairs = conf.fxList)(spark, env)
          .processDates(conf.fromDate, conf.toDate)
          .show(2000)
      case _ =>
        throw new Exception("not implemented")
    }
  }
}

class BackFillEnv(env: String) {
  // backfill fx symbol
  def doFxYear
    (yy: Int, symbol: String)(
    implicit spark : SparkSession) = {

    println(s"${yy} ...")

    val args =
      Array(
        "--env-name", env,
        "--backfill-year", yy.toString,
        "--job", "fetch-yahoo-prices",
        "--fx", symbol)

    ElementalJob.main(args)
  }

  // backfill stock symbol  
  def doStockYear
    (yy: Int, stock: String)(
    implicit spark : SparkSession) = {

    println(s"${yy} ...")

    val args =
      Array(
        "--env-name", env,
        "--backfill-year", yy.toString,
        "--job", "fetch-yahoo-prices",
        "--stocks", stock)

    ElementalJob.main(args)
  }

  def backfill(implicit spark : SparkSession) = {
    (2006 until 2022) foreach (yy => doFxYear(yy, "AUDUSD"))
    (2006 until 2022) foreach (yy => doStockYear(yy, "SPY"))
  }
}


