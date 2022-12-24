package usd.modeling.features

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import usd.data.config.ElementalTables
import usd.data.config.CcyEnv
import usd.modeling.RegressionInput
import org.apache.spark.sql.expressions.Window

class Returns(baseFeature: Feature, horizons: Int*) (implicit
  spark : SparkSession,
  env : CcyEnv with ElementalTables)
    extends Feature {

  import spark.implicits._

  val featureName = s"${baseFeature.featureName}_returns"

  def returnsName(hz: Int) = s"${featureName}_${hz}"
  def columnNames = horizons.map(returnsName _).toList

  def feature(input : RegressionInput) = {
    assert(baseFeature.columnNames.length == 1)
    val baseFeatName = baseFeature.columnNames.head
    val inputAug = baseFeature.feature(input).orderBy($"date" . asc)

    val wspec = Window.orderBy($"date".asc)
    val withReturns =
      horizons.foldLeft(inputAug){case (ri, hz) =>
        ri
          .withColumn("__lagged__", lag(col(baseFeatName), hz) over wspec)
          .withColumn(
            returnsName(hz),
            (col(baseFeatName) - $"__lagged__")/ $"__lagged__")
          .drop("__lagged__")
      }

    val right = withReturns.select("date", columnNames:_*)

    input.modelingDf.join(right, Seq("date"))
  }
}

