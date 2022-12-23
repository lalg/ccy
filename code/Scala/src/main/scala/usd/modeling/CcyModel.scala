package usd.modeling
import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import usd.models.LogisticRegressionTrainTest
import usd.modeling.features.{Feature, CcyPairFeatures}
import usd.data.config.{CcyEnv, ElementalTables}
import usd.data.config.CcyPairTables
import usd.data.io.{CcyDesignMatrixIo,PipelineModelIo}
import usd.data.source.CurrencyPairs
import usd.util.{DateUtils,CcyLogging}
import usd.apps.CcyPairConf
import usd.modeling.features.SecurityPrices

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
    logger.info("------------------")
    logger.info("TRAINING SUMMARY")
    logger.info()
  }
}


object CcyModel {
  def apply(conf: CcyPairConf)(implicit
    spark : SparkSession,
    env: CcyEnv with ElementalTables with CcyPairTables) = {

    val allFeatures =
      List(
        new SecurityPrices("SPY"))
    new CcyModel(conf, allFeatures)
  }
}

