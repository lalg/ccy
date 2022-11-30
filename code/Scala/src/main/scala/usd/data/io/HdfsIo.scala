package usd.data.io

import org.apache.spark.sql.SparkSession
import usd.util.CcyLogging
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

class HdfsStorageIo(
  val hdfsPath : String,
  val format : String = "parquet") (
  implicit spark : SparkSession)
    extends StorageIo with CcyLogging {


  def pathExists : Boolean = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.exists(new Path(hdfsPath))
  }

  def read = {
    logger.info(s"${hdfsPath} -- ${format} read")
    spark.read.format(format).load(hdfsPath)
  }

  def write(df: DataFrame, mode: SaveMode, partitionColumns: String*) = {
    logger.info(s"${hdfsPath} -- ${format} write")
    df.write
      .format(format)
      .partitionBy(partitionColumns: _*)
      .mode(mode)
      .save(hdfsPath)
  }
}

