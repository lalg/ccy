package usd.data.tables

import usd.data.config.Env
import org.apache.spark.sql.DataFrame
import usd.util.DateUtils
import org.apache.spark.sql.SaveMode

trait Elemental extends Env {
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
