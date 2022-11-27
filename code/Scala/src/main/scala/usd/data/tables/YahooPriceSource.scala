package usd.data.tables

import org.apache.spark.sql.SparkSession
import java.sql.Date
import java.util.Calendar
import yahoofinance.YahooFinance
import yahoofinance.histquotes.{Interval, HistoricalQuote}
import yahoofinance.histquotes2.IntervalMapper
import usd.util.DateUtils
import usd.util.CcyLogging
import org.apache.spark.sql.Dataset

class YahooPriceSource(implicit spark: SparkSession)
    extends SymbolPriceSource
    with CcyLogging {


  import scala.jdk.CollectionConverters._
  import spark.implicits._

  def dateToCalendar(dt: Date) = {
    val cal = Calendar.getInstance()
    cal.setTime(dt)
    cal
  }

  def getDate(date: Date, symbols: Seq[String]) : Dataset[Ohlc]= {
    val fromCal = dateToCalendar(date)
    val toCal = dateToCalendar(DateUtils(date).nextDay)
    logger.info(s"${fromCal} -- start date")
    logger.info(s"${toCal} -- end date")    
    getCalRange(fromCal, toCal, Interval.MONTHLY, symbols)
  }

  def getDateRange(startDate: Date, endDate: Date, symbols: Seq[String]) = {
    val fromCal = dateToCalendar(startDate)
    val toCal = dateToCalendar(endDate)
    getCalRange(fromCal, toCal, Interval.DAILY, symbols)
  }

  def getCalRange(
    fromCal: Calendar,
    toCal: Calendar,
    interval: Interval,
    symbols: Seq[String]) = {
    val stockMap =
      YahooFinance.get(symbols.toArray, fromCal, toCal, interval)
        .asScala

    symbols.foreach (symbol => logger.info(stockMap(symbol).toString()))

    val ohlc =
      symbols.map {case symbol =>
        val stock = stockMap(symbol).getHistory.asScala.head
        Ohlc.valueOf(stock)
      }

    ohlc.toDS()
  }


}

object YahooPriceSource {

}


// reference:
//   https://javadoc.io/doc/de.sfuhrm/YahooFinanceAPI/latest/yahoofinance/YahooFinance.html
