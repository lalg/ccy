package usd.modeling.features

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import usd.data.config.ElementalTables
import usd.data.config.CcyEnv
import usd.modeling.RegressionInput

class Returns(baseFeatName : String, horizons: Seq[Int]) (implicit
  spark : SparkSession,
  env : CcyEnv with ElementalTables)
    extends Feature {

  import spark.implicits._

  val featureName = s"${baseFeatName}_returns"

  def returnsName(hz: Int) = s"${featureName}_${hz}"
  def columnNames = horizons.map(returnsName _).toList

  def feature(input : RegressionInput) = {
    val base =  input.modelingDf.orderBy($"date" . asc)
    horizons.foldLeft(base){case (ri, hz) =>
      val lagged = lag(col(baseFeatName), hz)
        ri.withColumn(returnsName(hz), (col(baseFeatName) - lagged)/lagged)
    }
  }
}

