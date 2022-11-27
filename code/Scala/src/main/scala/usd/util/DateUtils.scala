package usd.util

import java.sql.Date
import java.sql.Timestamp
import java.util.Calendar
import java.util.TimeZone
import java.time.format.DateTimeFormatter
import java.time.LocalDate

case class DateUtils(dt: Date) {

  val nyTz = TimeZone.getTimeZone("America/New_York")

  private val cal = {
    val nyCal = Calendar.getInstance
    nyCal.setTimeZone(nyTz) 
    nyCal.setTime(dt)
    nyCal
  }

  private val localDate = dt.toLocalDate()

  def year = localDate.getYear()

  // zero based. 0 => January
  def month = localDate.getMonth()
  def date = localDate.getDayOfMonth()

  // Sun = 1, ... Sat=7
  def dayOfWeek = localDate.getDayOfWeek().getValue()
  def plusDays(days: Int) : Date = 
    Date.valueOf(localDate.plusDays(days))

  def nextDay = plusDays(1)
  def previousDay = plusDays(-1)
}



object DateUtils {
  def apply (ts: Timestamp) : DateUtils =
    DateUtils (new Date(ts.getTime()))

  def apply(dateStr: String) : DateUtils =
    DateUtils (Date.valueOf(dateStr))

}

  
  
