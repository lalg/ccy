package usd.models

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

import usd.data.io.PipelineModelIo
import usd.util.CcyLogging

trait LogisticRegressionTrainTest
    extends TrainTestModel
    with MlModel
    with ModelInput
    with Features
    with LogisticRegressionParams
    with CcyLogging {

  type EvaluationSummary = BinaryLogisticRegressionSummary
  type TrainingSummary = BinaryLogisticRegressionTrainingSummary
  type ModelTransformer = LogisticRegressionModel

  def buildPipeline = {
    val lr =
      new LogisticRegression()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setMaxIter(getMaxIteration)
        .setStandardization(false)
        .setFitIntercept(getFitIntercept)

    new Pipeline()
      .setStages(assemblerStages :+ lr)
  }

  def trainWith(input: DataFrame) = {
    val pipelineModel = fit(input)
    val lrm = modelTransformer(pipelineModel)
    val summary = lrm.binarySummary
    (pipelineModel, summary)
  }

  // training
  def train = trainWith(trainingInput)
  def finalTraining = trainWith(finalTrainingInput)
  def trainAndSave(pipelineIo : PipelineModelIo) =
    fitAndSave(trainingInput, pipelineIo)
  def finalTrainingAndSave(pipelineIo : PipelineModelIo) : Unit =
    fitAndSave(finalTrainingInput, pipelineIo)

  // testing
  def trainAndTest : TrainTestResult = {
    val pipelineModel = fit(trainingInput)
    val lrm = modelTransformer(pipelineModel)
    val trainSumm = lrm.binarySummary

    val preprocessSteps = pipelineModel.stages.dropRight(1)
    val preprocessed =
      preprocessSteps.foldLeft(testingInput) {
        case (dm, stage) => stage.transform(dm)
      }
    val testSumm = lrm.evaluate(preprocessed).asBinary

    (testSumm.predictions, lrm, trainSumm, testSumm)
  }

  // predictions
  def trainingPredictions(ts: TrainingSummary) = ts.predictions
  def predict(pipeModelIo : PipelineModelIo) =
    transform(predictionInput, pipeModelIo)

  def trainingSummary(mt: ModelTransformer, ts: TrainingSummary) = {
    List(
      "areaUnderROC" -> s"${ts.areaUnderROC}",
      "areaUnderPR" -> s"${areaUnderPr(ts)}",
      "features" ->
        (LogisticRegressionTrainTest.getFeatureNames(ts.predictions, mt.getFeaturesCol)
          .mkString(",")),
      "coefficients" -> mt.coefficients.toArray.mkString(","),
      "intercept" -> (if (mt.getFitIntercept) mt.intercept.toString else "0"))
    .toMap
  }

  def evaluationSummary(mt: ModelTransformer, es: EvaluationSummary) = {
    val predictionCounts = es.predictions.groupBy("predictions").count()
    val labelCounts = es.predictions.groupBy("label").count()

    List(
      "areaUnderROC" -> s"${es.areaUnderROC}",
      "areaUnderPR" -> s"${areaUnderPr(es)}",
      "accuracy" -> s"${es.accuracy%2.3f}",
      "FPR" -> s"${es.weightedFalsePositiveRate%2.3f}",
      "TPR" -> s"${es.weightedTruePositiveRate%2.3f}",
      "predictions" -> s"""${predictionCounts.collect().mkString(",")}""",
      "labels" -> s"""${labelCounts.collect().mkString(",")}""".stripMargin)
      .toMap
  }

  def areaUnderPr(bls: BinaryLogisticRegressionSummary) = {
    val scoreLabelRdd =
      bls.predictions
        .rdd
       .map {case row =>
          val prob = row.getAs[DenseVector]("probability")
          val score = prob.values.last
          val label = row.getAs[Double]("label")
          (score, label)
        }

    new BinaryClassificationMetrics(scoreLabelRdd)
      .areaUnderPR()
  }
}

object LogisticRegressionTrainTest {
  def getFeatureNames(
    predictions: DataFrame,
    featuresColumnNames:String = "features") : Array[String] = {

    AttributeGroup.fromStructField(predictions.schema(featuresColumnNames))
      .attributes
      .get
      .map(_.name.get)
  }
}

