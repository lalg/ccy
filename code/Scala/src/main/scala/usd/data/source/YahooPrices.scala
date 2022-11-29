package usd.data.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import java.sql.Date

trait AssetPrices {
  val stocks : Seq[String]
  val fxPairs : Seq[CurrencyPairs.CcyPair]

  def getDate(date: Date) : Dataset[Ohlc]
  def getDates(startDate: Date, endDate: Date) : Dataset[Ohlc]
}

trait YahooPrices extends AssetPrices  {
  val stocks: Seq[String]
  val fxPairs: seq[CurrencyPairs.CcyPair]

  implicit val  spark : SparkSession

  import spark.implicits._

  def yahooSymbols = {
    val fxSymbols = fxPairs map (ccyp => s"${ccyp.toString}=X")
    stocks ++ fxSymbols
  }

  val FxSymbolRegex = "(.*)=X".r

  val unmapFxUdf =
    udf((symbol: String) =>
      symbol match {
        case FxSymbolRegex(fxSymbol) => fxSymbol
        case _ => symbol
      })

  def getDate(date: Date) = {
    val df =  new YahooPriceSource().getDate(date, yahooSymbols)
    df.withColumn("symbol", unmapFxUdf($"symbol"))
  }

  def getDates(startDate: Date, endDate: Date) = {
    val df = new YahooPriceSource().getDateRange(startDate, endDate, yahooSymbols)
    df.withColumn("symbol", unmapFxUdf($"symbol"))
  }

}



