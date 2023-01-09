package usd.models

import usd.util.CcyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.VectorAssembler

class SimpleLinearRegression(
  designMatrix : DataFrame,
  yLabel: String,
  xLabels : Seq[String],
  intercept : Boolean = true)
    extends  LinearRegressionTrainTest
    with CcyLogging {

  def finalTrainingInput: DataFrame =
    if (yLabel.equals("label")) designMatrix
    else designMatrix.withColumn("label", col(yLabel))

  val notImplemented = new Exception("not implemented")
  def testingInput = throw notImplemented
  def trainingInput: DataFrame = throw notImplemented
  def predictionInput = throw notImplemented

  def assemblerStages = {
    val vectorAssembler =
      new VectorAssembler()
        .setInputCols(xLabels.toArray)
        .setOutputCol("features")

    Array(vectorAssembler)
  }

  def regression : Map[String, String] = {
    val (pipeModel, trainSumm) = finalTraining
    trainingSummary(modelTransformer(pipeModel), trainSumm)
  }
}
