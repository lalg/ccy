package usd.models

import org.apache.spark.sql.DataFrame

trait CcyFeature {
  val featureName : String

  val isCategorical : Boolean = false

  // new column names added to design matrix
  def columnNames : List[String]

  def feature(rInput : CcyRegressionInput) : DataFrame

  def build(rInput: CcyRegressionInput) : CcyRegressionInput =
    rInput.copy(
      dmDf = feature(rInput),
      featureColumns = columnNames ++ rInput.featureColumns)
}


trait CategoricalCcyFeature extends CcyFeature {
  def columnNames = List(featureName)
  override val isCategorical = true

}

trait ContinuousCcyFeature extends CcyFeature {
  def columnNames = List(featureName)
}
