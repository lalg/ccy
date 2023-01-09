package usd.models

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.regression._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.AttributeGroup
import usd.util.CcyLogging
import usd.data.io.PipelineModelIo

trait LinearRegressionTrainTest
    extends TrainTestModel
    with MlModel
    with ModelInput
    with Features
    with CcyLogging {

  type EvaluationSummary = LinearRegressionSummary
  type TrainingSummary = LinearRegressionTrainingSummary
  type ModelTransformer = LinearRegressionModel

  def buildPipeline = {
    val linearRegression =
      new LinearRegression()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setStandardization(false)
        .setFitIntercept(true)

    new Pipeline()
      .setStages(assemblerStages :+ linearRegression)
  }


  def trainWith(input : DataFrame) = {
    val pipelineModel = fit(input)
    val lrm = modelTransformer(pipelineModel)
    (pipelineModel, lrm.summary)
  }

  def train = trainWith(trainingInput)
  def finalTraining = trainWith(finalTrainingInput)

  def trainAndSave(pipelineIo: PipelineModelIo) =
    fitAndSave(trainingInput, pipelineIo)

  def finalTrainingAndSave(pipeModelIo: PipelineModelIo) =  
    fitAndSave(finalTrainingInput, pipeModelIo)

  def trainAndTest : TrainTestResult = {
    val pipelineModel = fit(trainingInput)
    val lrm = modelTransformer(pipelineModel)
    val trainSumm = lrm.summary
    val preprocessSteps = pipelineModel.stages.dropRight(1)
    val preprocessed =
      preprocessSteps.foldLeft(testingInput) {case(dm, stage) =>
        stage.transform(dm)}
    val evalSumm =
      lrm.evaluate(preprocessed).asInstanceOf[LinearRegressionSummary]

    (evalSumm.predictions, lrm, trainSumm, evalSumm)
  }

  def trainingPredictions(ts: TrainingSummary) = ts.predictions

  def predict(pipeModelIo : PipelineModelIo) =
    transform(predictionInput, pipeModelIo)

  def summaryMap(mt: ModelTransformer, ts: LinearRegressionSummary) =
    List(
      "features" -> vectorNames(mt, ts).mkString(","),
      "coefficients" -> mt.coefficients.toArray.mkString(","),
      "intercept" -> mt.intercept.toString(),
      "rsquared" -> s"${ts.r2}",
      "mse" -> s"${ts.meanSquaredError}",
      "rmse" -> s"${ts.rootMeanSquaredError}",
      "tValues" -> ts.tValues.mkString(","),
      "pValues" -> ts.pValues.mkString(","),
      "n" -> s"${ts.numInstances}")
      .toMap

  def trainingSummary(mt: ModelTransformer, ts: TrainingSummary) =
    summaryMap(mt, ts)

  def evaluationSummary(mt: ModelTransformer, es: EvaluationSummary) =
    summaryMap(mt, es)

  def vectorNames(mt: ModelTransformer, ts: LinearRegressionSummary) = {
    val schema = ts.predictions.schema
    AttributeGroup.fromStructField(schema(mt.getFeaturesCol))
      .attributes
      .get
      .map(_.name.get)
  }
}


