package usd.data.tables

import java.sql.Date
import yahoofinance.YahooFinance
import org.apache.spark.sql.DataFrame

trait FinanceSource {
  def getDate(date: Date, symbols: Seq[String]) : Unit
  def getDateRange(startDate: Date, endDate: Date, symbols: Seq[String]): DataFrame
}

