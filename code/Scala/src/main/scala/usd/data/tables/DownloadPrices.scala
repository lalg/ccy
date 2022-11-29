package usd.data.tables

import java.sql.Date
import org.apache.spark.sql.SparkSession
import usd.data.config.CcyEnv
import org.apache.spark.sql.SaveMode

class DownloadPrices(
  startDate: Date,
  endDate: Date,
  val stocks: Seq[String],
  val fxPairs : Seq[CurrencyPairs.CcyPair])
  (implicit
    spark : SparkSession,
    env: CcyEnv)
    extends AssetPrices {

  import spark.implicits._

  def download = getDates(startDate, endDate)

  def save(prices: Dataset[Ohlc]) =
    prices
      .write
      .partitionBy("year")
      .mode(SaveMode.Append)
  .parquet(
  
}



