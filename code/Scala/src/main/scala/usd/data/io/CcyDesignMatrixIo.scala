package usd.data.io

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import usd.data.config.CcyEnv
import java.sql.Date
import usd.util.CcyLogging
import usd.util.DateUtils

class CcyDesignMatrixIo(val modelName : String)
  (implicit
    spark: SparkSession,
    env : CcyEnv)
    extends CcyLogging {


  import spark.implicits._

  val designMatrixPath = s"${env.env.hdfsRoot}/${modelName}_dm"
  val io = new HdfsStorageIo(designMatrixPath)

  def containsInterval(startDate: Date, endDate: Date) : Boolean = {
    def withinRange = {
      val minMax =
        io.read
          .agg(
            min("date") as "min_date",
            max("date") as "max_date")
          .head()

      val dmStart = minMax.getAs[Date]("min_date")
      val dmEnd =
        new DateUtils(minMax.getAs[Date]("max_date"))
          .plusDays(1)

      logger.info(s"[${dmStart}, ${dmEnd}) -- saved")
      logger.info(s"[${startDate}, ${endDate}) -- requested")    

      dmStart.compareTo(startDate) <= 0 &&
      dmEnd.compareTo(endDate) >= 0
    }
    io.pathExists && withinRange
  }

  def getSavedOpt(startDate: Date, endDate: Date) =
    if (!containsInterval(startDate, endDate)) None
    else
      Some(
        io.read
          .filter($"date" >= startDate && $"date" < endDate))
}

