package usd.data.io

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

trait StorageIo {
  def read : DataFrame
  def write(df: DataFrame, mode: SaveMode, partitionColumns: String*) : Unit

  // overridden where appropriate
  def readPath(paths: String*) : Option[DataFrame] = None
}



