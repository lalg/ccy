package usd.modeling.features

import org.apache.spark.sql.SparkSession
import usd.data.config.CcyEnv
import usd.data.config.ElementalTables
import usd.modeling.RegressionInput
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class MovingAverage(baseColumnName: String, horizons: Int *)(implicit
  spark : SparkSession,
  env: CcyEnv with ElementalTables)
    extends Feature {

  import spark.implicits._

  // for simple moving average
  val featureName = s"${baseColumnName}_sma"
  def movingAvgName(hz: Int) = s"${featureName}_${hz}"
  def columnNames = horizons.map(movingAvgName _).toList

  def feature(input: RegressionInput) = {
    horizons.foldLeft (input.modelingDf) { case (ri, hz) =>
      val wspec =
        Window.orderBy(col("date").asc)
          .rowsBetween(-hz + 1, Window.currentRow)

      ri.withColumn(
        movingAvgName(hz),
        avg(baseColumnName) over wspec)
    }
  }
}

