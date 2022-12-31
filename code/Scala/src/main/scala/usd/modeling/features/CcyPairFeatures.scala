package usd.modeling.features

import org.apache.spark.sql.SparkSession
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
  def designMatrixIo : CcyDesignMatrixIo

  implicit val spark : SparkSession
  implicit val env : CcyEnv

  import spark.implicits._

  def stringIndexers =
    features.filter(_.isCategorical)
      .flatMap(f =>
        f.columnNames.map (col =>
          new StringIndexer()
            .setInputCol(col)
            .setOutputCol(s"${col}__indexer")
            .setHandleInvalid("keep")))
      .toArray

  def oneHotEncoders =
    stringIndexers
      .map(si =>
        new OneHotEncoder()
          .setInputCol(si.getOutputCol)
          .setOutputCol(s"${si.getInputCol}__vec"))

  def continuousAssember = {
    val continousNames =
      features
        .filter(f => !f.isCategorical)
        .flatMap(_.columnNames)
        .toArray

    new VectorAssembler()
      .setInputCols(continousNames)
//      .setOutputCol("contfeat")
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
}



