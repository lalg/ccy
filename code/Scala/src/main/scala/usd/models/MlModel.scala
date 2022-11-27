package usd.models

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import usd.data.io.PipelineModelIo

// spark ML pipeline interface agnostic to specific learning algorithm used

trait MlModel {

  def buildPipeline : Pipeline

  def fit(designMatrix : DataFrame) : PipelineModel =
    buildPipeline.fit(designMatrix)

  def fitAndSave(designMatrix: DataFrame, modelIo: PipelineModelIo) = 
    modelIo.write(fit(designMatrix))

  def fitAndTransform(trainMatrix: DataFrame, testMatrix: DataFrame) : DataFrame  =
    fit(trainMatrix)
      .transform(testMatrix)

  def transform(testMatrix : DataFrame, modelIo: PipelineModelIo) : DataFrame =
    // exception if model not previously saved
    modelIo.read.transform(testMatrix)
}


