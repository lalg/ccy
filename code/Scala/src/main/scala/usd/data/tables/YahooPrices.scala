package usd.data.tables

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import java.sql.Date

class YahooPrices(
  stocksOpt: Option[Seq[String]],
  fxOpt: Option[Seq[CurrencyPairs.CcyPair]])(implicit
    spark : SparkSession) {

  import spark.implicits._

  def yahooSymbols = {
    val fxSymbols = fxOpt.map(fxSeq => fxSeq map (fx => s"${fx.toString}=X"))
    stocksOpt.getOrElse(Seq.empty) ++ fxSymbols.getOrElse(Seq.empty)
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



