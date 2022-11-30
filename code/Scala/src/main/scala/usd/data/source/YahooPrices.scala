package usd.data.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import java.sql.Date

trait YahooPrices extends AssetPrices  {
  val stocks: Seq[String]
  val fxPairs: Seq[CurrencyPairs.CcyPair]

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
    new YahooPriceSource().getDate(date, yahooSymbols)
      .withColumn("symbol", unmapFxUdf($"symbol"))
      .as[Ohlc]

  }

  def getDates(startDate: Date, endDate: Date) = {
    new YahooPriceSource().getDateRange(startDate, endDate, yahooSymbols)
      .withColumn("symbol", unmapFxUdf($"symbol"))
      .as[Ohlc]
  }

}



