package usd.modeling.features

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col}
import usd.data.config.{CcyEnv, ElementalTables}
import usd.modeling.RegressionInput

class MovingAvg(baseFeature : Feature, horizons: Int *) (implicit
  spark: SparkSession,
  env: CcyEnv with ElementalTables)
    extends Feature {

  import spark.implicits._

  val featureName = s"${baseFeature.featureName}_movingAvg"
  def movingAvgName(hz: Int) = s"${featureName}_${hz}"
  def columnNames = horizons.map(movingAvgName _).toList


  def feature(input: RegressionInput) = {
    val inputFeature = baseFeature.feature(input)

    val wspec = 

    assert(baseFeature.columnNames.length == 1)
    val baseFeatCol = baseFeature.columnNames.head

    val withAverages =
      horizons.foldLeft(inputFeature){case (ri, hz) =>
        val wspec =
          Window.orderBy(col("date").asc)
            .rowsBetween(-hz + 1, Window.currentRow)
        ri
          .withColumn(
            movingAvgName(hz), avg(baseFeatCol) over wspec)
      }

    val right = withAverages.select("date", columnNames: _*)

    input.modelingDf.join(right, Seq("date"))
  }
}

