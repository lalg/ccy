package usd.data.source

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

  // stupid yahoo returns an extra day for FX symbols
  def filterToRange(df: Dataset[Ohlc], startDate: Date, endDate: Date) = 
    df.filter(df("date") >= startDate && df("date") < endDate) 

  def getDate(startDate: Date, symbols: Seq[String]) : Dataset[Ohlc]= {
    val nextDate = DateUtils(startDate).nextDay
    val fromCal = dateToCalendar(startDate)
    val toCal = dateToCalendar(nextDate)
    logger.info(s"${fromCal} -- start date")
    logger.info(s"${toCal} -- end date")
    filterToRange(
      getCalRange(fromCal, toCal, Interval.DAILY, symbols),
      startDate,
      nextDate)
  }

  def getDateRange(startDate: Date, endDate: Date, symbols: Seq[String]) = {
    val fromCal = dateToCalendar(startDate)
    val toCal = dateToCalendar(endDate)
    filterToRange(
      getCalRange(fromCal, toCal, Interval.DAILY, symbols),
      startDate,
      endDate)
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
      symbols.foldLeft(List.empty[HistoricalQuote]) {
        case (acc, symbol) =>
          stockMap(symbol).getHistory().asScala.toList ++ acc
      }

    ohlc.flatMap (Ohlc.valueOf _)
      .toSeq
      .toDS()
  }
}

object YahooPriceSource {

}


// reference:
//   https://javadoc.io/doc/de.sfuhrm/YahooFinanceAPI/latest/yahoofinance/YahooFinance.html
