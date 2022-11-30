package usd.data.source

import java.sql.Date
import org.apache.spark.sql.Dataset

trait AssetPrices {
  val stocks : Seq[String]
  val fxPairs : Seq[CurrencyPairs.CcyPair]

  def getDate(date: Date) : Dataset[Ohlc]
  def getDates(startDate: Date, endDate: Date) : Dataset[Ohlc]
}

