package usd.apps

import org.apache.spark.sql.SparkSession

case class SparkBase(appName: String) {
  val spark =
    SparkSession
      .builder()
      .config("spark.master", "local")
      .config("spark.sql.orc.filterPushdown", "true")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.speculation", "false")
      .appName(appName)
      .getOrCreate()
}


