package usd.modeling.features

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.VectorAssembler
import usd.data.config.CcyEnv
import usd.models.Features
import usd.models.LogisticRegressionParams
import usd.util.CcyLogging
import usd.data.io.CcyDesignMatrixIo
import usd.modeling.RegressionInput
import org.apache.spark.sql.SaveMode

trait CcyPairFeatures
    extends Features
    with LogisticRegressionParams
    with CcyLogging {

  val features : List[Feature]
  val featuresSelected: List[String]
  def designMatrixIo : CcyDesignMatrixIo

  implicit val spark : SparkSession
  implicit val env : CcyEnv

  import spark.implicits._

  def featureIsSelected(f: String) = 
    featuresSelected.isEmpty || featuresSelected.contains(f)

  def stringIndexers =
    features.filter(_.isCategorical)
      .flatMap(f => f.columnNames)
      .flatMap(f => if (featureIsSelected(f)) Some(f) else None)
      .map(f =>
          new StringIndexer()
            .setInputCol(f)
            .setOutputCol(s"${f}__indexer")
            .setHandleInvalid("keep"))
      .toArray 


  def oneHotEncoders =
    stringIndexers
      .map(si =>
        new OneHotEncoder()
          .setInputCol(si.getOutputCol)
          .setOutputCol(s"${si.getInputCol}__vec"))

  def continuousAssember = {
    val continuosNames =
      features
        .filter(f => !f.isCategorical)
        .flatMap(_.columnNames)
        .flatMap(f => if (featureIsSelected(f)) Some(f) else None)
        .toArray

    new VectorAssembler()
      .setInputCols(continuosNames)
      .setOutputCol("cf")
  }

  def assemblerStages: Array[PipelineStage] = {
    val si = stringIndexers
    val ohe = oneHotEncoders
    val ca = continuousAssember

    val oheCols = ohe.map(_.getOutputCol)
    val outCols = ca.getOutputCol +: oheCols
    val assembler =
      new VectorAssembler()
        .setInputCols(outCols)
        .setOutputCol("features")

    (si ++ ohe) :+ ca :+ assembler
  }

  def designMatrix(baseRi : RegressionInput) : RegressionInput = {
    val presentColumns = baseRi.featureColumns.toSet

    def hasColumns(colNames : List[String]) =
      colNames
        .map(colName => presentColumns.contains(colName))
        .reduce(_ && _)

    def processFeatures = 
      features.foldLeft(baseRi) {
        case (ri: RegressionInput, feat: Feature) =>
          if (hasColumns(feat.columnNames)) {
            logger.info(s"{feat.featureName} -- skipping")
            ri
          }
          else feat.transform(ri)
      }

    val count0 = baseRi.modelingDf.count()
    val riFilled = processFeatures
    val newFeatures = riFilled.modelingDf.cache()
    val count = newFeatures.count()

    assert(
      count != 0 && count == count0,
      "malformed design matrix")
    riFilled
  }

  def buildAndSaveDesignMatrix(baseRi : RegressionInput) = 
    designMatrixIo.io.write(
      designMatrix(baseRi).modelingDf,
      SaveMode.Overwrite)


  // for now only continuous features
  def crossCorrelationOLD(baseRi: RegressionInput) : DataFrame = {
    val filledRi = designMatrix(baseRi)
    val featurePairs =
      filledRi.featureColumns.flatMap ((f : String) =>
        if (featureIsSelected(f)) Some(f) else None)
        .combinations(2)
        .toList

    val dm = filledRi.modelingDf

    def correlation(f1: String, f2: String) =
      dm.agg(corr(f1, f2) as "__corr__")
        .head()
        .getAs[Double]("__corr__")

    featurePairs
      .map (_.toList match {
        case f1::f2::Nil => (f1, f2, correlation(f1, f2))
        case _ => throw new Exception("impossible")
      })
    .toDF("f1", "f2", "correlation")
  }

  def crossCorrelation(ri: RegressionInput) : DataFrame = {
    val featurePairs = ri.featureColumns.combinations(2).toList

    val dm = ri.modelingDf

    def correlation(f1: String, f2: String) =
      dm.agg(corr(f1, f2) as "__corr__")
        .head()
        .getAs[Double]("__corr__")

    featurePairs
      .map (_.toList match {
        case f1::f2::Nil => (f1, f2, correlation(f1, f2))
        case _ => throw new Exception("impossible")
      })
    .toDF("f1", "f2", "correlation")
  }
}



