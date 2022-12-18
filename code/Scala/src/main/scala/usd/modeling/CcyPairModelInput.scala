package usd.modeling

import java.sql.Date
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import usd.data.config.{CcyEnv, ElementalTables}
import usd.data.source.CurrencyPairs
import usd.apps.CcyPairConf

//labels based on open price, so that during prediction
//   data for open, high, and low are available.
//   close is missing
class CcyPairModelInput
  (implicit
    spark : SparkSession,
    env : CcyEnv with ElementalTables) {

  import spark.implicits._

  def baseModelingData(
    ccyPair: CurrencyPairs.CcyPair,
    startDate: Date,
    endDate: Date,
    daysInHorizon : Int) = {

    val wspec = Window.orderBy($"date".asc)

    val df =
      env.yahooPrices.read
        .filter(
          $"symbol" === ccyPair.toString() &&
            $"date" >= startDate && $"date" < endDate)
        .withColumn("lagged", lead("open", daysInHorizon) over wspec)
        .na.drop(Array("lagged"))
        .withColumn("label", when($"lagged" > $"open", 1).otherwise(0))
        .drop("lagged")
        .cache()

    new RegressionInput(
      startDate = startDate,
      endDate = endDate,
      featureColumns = Seq.empty,
      modelingDf = df,
      rowCount = df.count())
  }
}

