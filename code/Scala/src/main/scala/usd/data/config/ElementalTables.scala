package usd.data.config

import usd.data.io.HdfsStorageIo
import usd.data.tables.FetchYahooPrices
import org.apache.spark.sql.SparkSession

trait ElementalTables extends CcyEnv {
  def hdfsPath(tableName: String) = s"${env.hdfsRoot}/${tableName}"

  val yahooPrices = new HdfsStorageIo(hdfsPath(FetchYahooPrices.tableName))
}

