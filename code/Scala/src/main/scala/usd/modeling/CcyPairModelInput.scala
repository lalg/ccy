package usd.modeling

import org.apache.spark.sql.SparkSession
import usd.data.config.{CcyEnv, ElementalTables}
import usd.data.source.CurrencyPairs
import java.sql.Date

trait CcyPairModelInput {
  implicit val spark : SparkSession
  implicit val env : CcyEnv with ElementalTables

  import spark.implicits._

  def baseModelingData(
    ccyPair: CurrencyPairs.CcyPair,
    startDate: Date,
    endDate: Date) = {

    val df =
      env.yahooPrices.read
        .filter(
          $"symbol" === ccyPair.toString() &&
            $"date" >= startDate && $"date" < endDate)

    new RegressionInput(
      startDate = startDate,
      endDate = endDate,
      featureColumns = Seq.empty,
      modelingDf = df)
  }
}

