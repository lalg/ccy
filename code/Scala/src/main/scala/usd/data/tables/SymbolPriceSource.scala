package usd.data.tables

import java.sql.Date
import yahoofinance.YahooFinance
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

// terrible name.
//   should be something like TradedSymbolsSource or SymbolsPriceSource
trait SymbolPriceSource {
  def getDate(date: Date, symbols: Seq[String]) : Dataset[Ohlc]
  def getDateRange(startDate: Date, endDate: Date, symbols: Seq[String]): Dataset[Ohlc]
}

