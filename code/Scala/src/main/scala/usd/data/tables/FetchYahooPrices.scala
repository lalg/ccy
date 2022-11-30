package usd.data.tables
 
import java.sql.Date
import org.apache.spark.sql.SparkSession
import usd.data.config.CcyEnv
import org.apache.spark.sql.SaveMode
import usd.data.source.YahooPrices
import usd.data.source.CurrencyPairs
import org.apache.spark.sql.DataFrame
import usd.data.io.HdfsStorageIo
import usd.util.CcyLogging

// end date being absent means one day duration
class FetchYahooPrices(
  val stocks: Seq[String],
  val fxPairs : Seq[CurrencyPairs.CcyPair])
  (implicit
    val spark : SparkSession,
    env: CcyEnv)
    extends YahooPrices
    with Elemental
    with CcyLogging {


  import spark.implicits._
  import org.slf4j.LoggerFactory

  val partitionColumns = Seq("year")
  val storageIo =
    new HdfsStorageIo(hdfsPath=s"${env.env.hdfsRoot}/${FetchYahooPrices.tableName}")

  def processDates(startDate: Date, endDate: Date)  : DataFrame = {
    logger.info(s"{[${startDate},${endDate}) - process dates  range")
    getDates(startDate, endDate)
      .toDF()
  }
}

object FetchYahooPrices {
  val tableName = "yahoo-prices"
}







