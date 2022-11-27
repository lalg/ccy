package usd.data.tables

import org.apache.spark.sql.SparkSession
import java.sql.Date
import java.util.Calendar
import yahoofinance.YahooFinance
import yahoofinance.histquotes.{Interval, HistoricalQuote}
import usd.util.DateUtils
import usd.util.CcyLogging
import org.apache.spark.sql.Dataset


case class Ohlc(
  date : Date,
  symbol: String,
  open : Double,
  high : Double,
  low  : Double,
  close : Double,
  volume : Long)

object Ohlc {
  def valueOf(stock: HistoricalQuote) =
    Ohlc(
      date = new Date(stock.getDate.getTimeInMillis()),
      symbol = stock.getSymbol,
      open = stock.getOpen.doubleValue,
      high = stock.getHigh.doubleValue,
      low = stock.getLow.doubleValue,
      close = stock.getClose.doubleValue,
      volume = stock.getVolume)
}


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

    val stockMap =
      YahooFinance.get(symbols.toArray, fromCal, toCal)
        .asScala

    symbols.foreach (symbol => logger.info(stockMap(symbol).toString()))

    // only one date present
    symbols.foreach {case symbol =>
      assert(stockMap(symbol).getHistory.asScala.size == 1)
    }

    val ohlc =
      symbols.map {case symbol =>
        val stock = stockMap(symbol).getHistory.asScala.head
        Ohlc.valueOf(stock)
      }

    ohlc.toDS()
  }

  def getDateRange(startDate: Date, endDate: Date, symbols: Seq[String]) = ???

  def getDateRangeOLD(startDate: Date, endDate: Date, symbols: Seq[String]) = {
    val from = dateToCalendar(startDate)
    val to = dateToCalendar(endDate)
    val intvl = Interval.DAILY
    val stockMap =
      YahooFinance.get(symbols.toArray, from, to, intvl)
        .asScala
    stockMap
  }

}


// reference:
//   https://javadoc.io/doc/de.sfuhrm/YahooFinanceAPI/latest/yahoofinance/YahooFinance.html
