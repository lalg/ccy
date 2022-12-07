package usd.data.source

import yahoofinance.histquotes.HistoricalQuote
import java.sql.Date
import usd.util.DateUtils
import usd.util.CcyLogging

case class Ohlc(
  year : Int,
  date : Date,
  symbol: String,
  open : Double,
  high : Double,
  low  : Double,
  close : Double,
  volume : Long)

object Ohlc extends CcyLogging {
  def valueOf(stock: HistoricalQuote) = try {
    val dt = new Date(stock.getDate.getTimeInMillis())
    Some(
      Ohlc(
        date = dt,
        year = DateUtils(dt).year,
        symbol = stock.getSymbol,
        open = stock.getOpen.doubleValue,
        high = stock.getHigh.doubleValue,
        low = stock.getLow.doubleValue,
        close = stock.getClose.doubleValue,
        volume = stock.getVolume().longValue()))
  } catch {
    case e : Exception => {
      logger.info(s"${stock.getDate} -- bad data")
      None
    }
  }
}


