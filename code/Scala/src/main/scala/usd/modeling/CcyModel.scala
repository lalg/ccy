package usd.modeling
import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary

import usd.models.LogisticRegressionTrainTest
import usd.modeling.features.{Feature, CcyPairFeatures}
import usd.data.config.{CcyEnv, ElementalTables}
import usd.data.config.CcyPairTables
import usd.data.io.{CcyDesignMatrixIo,PipelineModelIo}
import usd.data.source.CurrencyPairs
import usd.util.{DateUtils,CcyLogging}
import usd.apps.CcyPairConf
import usd.modeling.features.SecurityPrices
import usd.modeling.features.Returns


class CcyModel(
  val conf : CcyPairConf,
  val features : List[Feature])(
  implicit
    val spark : SparkSession,
    val env : CcyEnv with ElementalTables with CcyPairTables)
    extends LogisticRegressionTrainTest
    with CcyPairFeatures
    with CcyLogging {

  val ccyPair = conf.ccyPair
  val modelName = s"${env.env.envName}_${ccyPair}"

  def designMatrixIo = new CcyDesignMatrixIo(modelName)

  val predictionModel = env.ccyPairModel(modelName, ccyPair)

  // Members declared in usd.models.ModelInput
  val modelInput = new CcyPairModelInput

  def buildDesignMatrix(startDate: Date, endDate: Date) = {
    val ri =
      modelInput.baseModelingData(
        ccyPair, startDate, endDate, conf.horizon)

    designMatrix(ri).modelingDf
    // DELETE
      .na.drop("any")

  }

  def testingInput =
    buildDesignMatrix(conf.testStartDate, conf.testEndDate)
  def trainingInput =
    buildDesignMatrix(conf.trainStartDate, conf.trainEndDate)
  def finalTrainingInput =
    buildDesignMatrix(conf.trainStartDate, conf.testEndDate)
  def predictionInput = {
    val predictionDate = DateUtils(conf.predictionDt()).dt
    val startDate =
      new DateUtils(predictionDate).plusDays(-conf.horizon)
    buildDesignMatrix(startDate, predictionDate)

  }

  def evaluate() = {
    val (preds, modelTrans, trainSumm, evalSumm) = trainAndTest

    // training
    logger.info("TRAINING SUMMARY")
    logger.info("----------------")
    logTrainingSummary(modelTrans, trainSumm)

    val objectiveHistory = trainSumm.objectiveHistory
    logger.info("OBJECTIVE HISTORY:")
    logger.info("------------------")
    objectiveHistory.foreach(loss => logger.info(loss))

    logger.info("TESTING SUMMARY:")
    logger.info("----------------")
    logEvaluationSummary(modelTrans, evalSumm)
    showPrecisionRecall(evalSumm)
  }

  def logTrainingSummary(mt:
      ModelTransformer, ts: TrainingSummary) = {
    val kv = trainingSummary(mt, ts)
    val names = kv("features").split(",")
    val coeffs = kv("coefficients").split(",")
    logger.info(s"""areaUnderROC"\t\t ${kv("areaUnderROC")}""")
    logger.info(s"""areaUnderPR:\t\t ${kv("areaUnderPR")}""")
    logger.info(s"""intercept:\t\t ${kv("intercept")}""")
    logger.info("coefficients:")
    names.zip(coeffs) foreach {case (k, v) => logger.info(s"WHY\t$k: \t$v")}
  }

  def logEvaluationSummary(mt: ModelTransformer, es: EvaluationSummary) =
    evaluationSummary(mt, es)
      .foreach {case (k,v) => logger.info(s"\t$k: \t$v")}

  def precisionRecall(binarySummary: BinaryLogisticRegressionSummary) =
    binarySummary.precisionByThreshold
      .join(binarySummary.recallByThreshold, Seq("threshold"))
      .join(binarySummary.fMeasureByThreshold, Seq("threshold"))
      .orderBy(col("threshold").desc)

  def showPrecisionRecall(binarySummary: BinaryLogisticRegressionSummary) = {
    val pr = precisionRecall(binarySummary)
    pr.show(pr.count().toInt, false)
  }
}


object CcyModel {
  def apply(conf: CcyPairConf)(implicit
    spark : SparkSession,
    env: CcyEnv with ElementalTables with CcyPairTables) = {

    val spy = new SecurityPrices("SPY")
    val allFeatures =
      List(
        new Returns(spy, 10))
        
    new CcyModel(conf, allFeatures)
  }
}

