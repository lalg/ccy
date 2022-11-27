package usd.util

import java.sql.Timestamp
import java.util.TimeZone
import java.sql.Date
import java.time.temporal.ChronoUnit

case class DateComponents(year: Int, month: Int, day: Int) {
  def encode = f"${year}${month}%02d${day}%02d"
}


// defaults to America/New York
case class TimestampUtil(
  ts: Timestamp,
  tz: TimeZone = TimeZone.getTimeZone("America/New_York"))
    extends Serializable {
}

object TimestampUtil {
  def daysBetween (tsStart: Timestamp, tsEnd: Timestamp) =
    ChronoUnit.DAYS.between(tsStart.toLocalDateTime, tsEnd.toLocalDateTime())

  def apply(dateStr: String) : TimestampUtil =
    TimestampUtil(Timestamp.valueOf(s"${dateStr} 00:00:00"))
}
