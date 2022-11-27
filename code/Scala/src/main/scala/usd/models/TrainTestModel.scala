package usd.models

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.PipelineModel
import usd.data.io.PipelineModelIo

trait TrainTestModel {
  type EvaluationSummary
  type TrainingSummary
  type ModelTransformer
  type TrainTestResult =
    (DataFrame, ModelTransformer, TrainingSummary, EvaluationSummary)

  // for summary console output
  def trainingSummary(mt: ModelTransformer, ts: TrainingSummary) : Map[String, String]
  def evaluationSummary(mt: ModelTransformer, es: EvaluationSummary) : Map[String, String]

  def modelTransformer(pm: PipelineModel) =
    pm.stages.last.asInstanceOf[ModelTransformer]

  // training
  def train : (PipelineModel, TrainingSummary)
  def finalTraining : (PipelineModel, TrainingSummary)

  // train and save
  def trainAndSave(pipeModelIo : PipelineModelIo) : Unit
  def finalTrainingAndSave(pipeModelIo: PipelineModelIo) : Unit

  // train and test
  def trainAndTest : TrainTestResult

  // predictions
  def predict(pipeModelIo: PipelineModelIo) : DataFrame
  def trainingPredictions(ts: TrainingSummary) : DataFrame

}
