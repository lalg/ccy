package usd.modeling.features

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import usd.data.config.ElementalTables
import usd.data.config.CcyEnv
import usd.modeling.RegressionInput
import org.apache.spark.sql.expressions.Window

class Returns(baseColumnName: String, horizons: Int*) (implicit
  spark : SparkSession,
  env : CcyEnv with ElementalTables)
    extends Feature {

  import spark.implicits._

  val featureName = s"${baseColumnName}_rets"

  def returnsName(hz: Int) = s"${featureName}_${hz}"
  def columnNames = horizons.map(returnsName _).toList

  def feature(input : RegressionInput) = {
    val wspec = Window.orderBy($"date".asc)
    horizons.foldLeft(input.modelingDf) { case (ri, hz) =>
      ri
        .withColumn("__lagged__", lag(col(baseColumnName), hz) over wspec)
        .withColumn(
          returnsName(hz),
          (col(baseColumnName) - $"__lagged__")/ $"__lagged__")
        .drop("__lagged__")
    }
  }
}

