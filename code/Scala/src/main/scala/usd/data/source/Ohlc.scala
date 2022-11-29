package usd.data.source

import yahoofinance.histquotes.HistoricalQuote
import java.sql.Date
import usd.util.DateUtils

case class Ohlc(
  year : Int,
  date : Date,
  symbol: String,
  open : Double,
  high : Double,
  low  : Double,
  close : Double,
  volume : Long)

object Ohlc {
  def valueOf(stock: HistoricalQuote) = {
    val dt = new Date(stock.getDate.getTimeInMillis())
    Ohlc(
      date = dt,
      year = DateUtils(dt).year,
      symbol = stock.getSymbol,
      open = stock.getOpen.doubleValue,
      high = stock.getHigh.doubleValue,
      low = stock.getLow.doubleValue,
      close = stock.getClose.doubleValue,
      volume = stock.getVolume)
  }
}

