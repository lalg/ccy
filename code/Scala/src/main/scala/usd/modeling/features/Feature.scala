package usd.modeling.features

import org.apache.spark.sql.DataFrame
import usd.modeling.RegressionInput

trait Feature {
  val featureName : String
  def isCategorical : Boolean = false

  def columnNames : List[String]
  def feature(rInput: RegressionInput) : DataFrame
  def transform(rInput: RegressionInput) : RegressionInput =
    rInput.copy(
      modelingDf = feature(rInput),
      featureColumns = columnNames ++ rInput.featureColumns)
}

trait CategoricalFeature extends Feature {
  def columnName = List(featureName)
  override def isCategorical = true
}

trait ContinuousFeature extends Feature {
  def columnNames = List(featureName)
}
