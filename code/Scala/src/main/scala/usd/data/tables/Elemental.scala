package usd.data.tables

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import java.sql.Date
import usd.util.DateUtils
import usd.data.config.CcyEnv
import usd.data.io.StorageIo
import org.apache.spark.sql.DataFrame
import usd.util.CcyLogging

trait Elemental  extends CcyLogging {
  // abstract
  val storageIo : StorageIo
  val partitionColumns : Seq[String]
  def processDates(startDate: Date, endDate: Date) : DataFrame

  def processDate(startDate: Date) : DataFrame = {
    logger.info(s"${startDate} -- processDate")
    val nextDay = DateUtils(startDate).nextDay
    processDates(startDate, nextDay)
  }

  def processDatesAndSave(startDate: Date, endDate: Date) : Unit = {
    logger.info(s"[${startDate},${endDate}) -- processDates")
    val df = processDates(startDate, endDate)
    storageIo.write(df, SaveMode.Append, partitionColumns:_*)
  }
}
