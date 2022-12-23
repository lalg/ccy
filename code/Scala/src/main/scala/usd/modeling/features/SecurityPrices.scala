package usd.modeling.features

import usd.modeling.RegressionInput
import org.apache.spark.sql.SparkSession
import usd.data.config.CcyEnv
import usd.data.config.ElementalTables

class SecurityPrices(val symbol : String)(implicit
  spark : SparkSession,
  env : CcyEnv with ElementalTables) 
    extends ContinuousFeature {

  import spark.implicits._

  val featureName = s"${symbol}-open"
  def feature (ri: RegressionInput) = {
    val openPx = 
      env.yahooPrices.read
        .filter(
          $"symbol" === symbol &&
            $"date" >= ri.startDate &&
            $"date" < ri.endDate)
        .select($"date", $"open" as featureName)

    ri.modelingDf
      .join(openPx, Seq("date"), "left")
  }
}




