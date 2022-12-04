package usd.data.tables
 
import java.sql.Date
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import usd.data.config.CcyEnv
import usd.data.source.CurrencyPairs
import usd.data.io.HdfsStorageIo
import usd.util.CcyLogging
import usd.data.source.YahooPriceSource
import usd.data.source.Ohlc


// end date being absent means one day duration
class FetchYahooPrices(
  val stocks: Seq[String],
  val fxPairs : Seq[CurrencyPairs.CcyPair])
  (implicit
    val spark : SparkSession,
    env: CcyEnv)
    extends Elemental
    with CcyLogging {


  import spark.implicits._

  def yahooSymbols = {
    val fxSymbols = fxPairs map (ccyp => s"${ccyp.toString}=X")
    stocks ++ fxSymbols
  }

  val partitionColumns = Seq("year")
  val FxSymbolRegex = "(.*)=X".r

  val unmapFxUdf =
    udf((symbol: String) =>
      symbol match {
        case FxSymbolRegex(fxSymbol) => fxSymbol
        case _ => symbol
      })

  val storageIo =
    new HdfsStorageIo(
      hdfsPath=s"${env.env.hdfsRoot}/${FetchYahooPrices.tableName}/")

  def processDates(startDate: Date, endDate: Date) = {
    logger.info(s"{[${startDate},${endDate}) - process dates  range")
    new YahooPriceSource().getDateRange(startDate, endDate, yahooSymbols)
      .withColumn("symbol", unmapFxUdf($"symbol"))
  }

}

object FetchYahooPrices {
  val tableName = "yahoo-prices"
}







