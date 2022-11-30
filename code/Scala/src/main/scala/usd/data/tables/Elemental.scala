package usd.data.tables

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import java.sql.Date
import usd.util.DateUtils
import usd.data.config.CcyEnv
import usd.data.io.StorageIo

trait Elemental  {
  // abstract
  val storageIo : StorageIo
  val partitionColumns : Seq[String]
  def processDates(startDate: Date, endDate: Date) : DataFrame

  def processDate(startDate: Date) : DataFrame = {
    val nextDay = DateUtils(startDate).nextDay
    processDates(startDate, nextDay)
  }

  def processDatesAndSave(startDate: Date, endDate: Date) : Unit = {
    val df = processDates(startDate, endDate)
    storageIo.write(df, SaveMode.Append, partitionColumns:_*)
  }
}
