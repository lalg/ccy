package usd.models

import org.apache.spark.sql.DataFrame

// each dataframe mentioned is a design matrix
trait ModelInput {
  def trainingInput : DataFrame
  def testingInput : DataFrame
  def finalTrainingInput : DataFrame
  def predictionInput : DataFrame
}


