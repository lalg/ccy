package usd.data.io

import org.apache.spark.sql.SparkSession

class CcyDesignMatrixIo(val modelName : String)(
  implicit
    spark: SparkSession) {

  import spark.implicits._

  val designMatrixName = s"${modelName}_dm"

}

