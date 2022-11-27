package usd.models

import org.apache.spark.ml.PipelineStage

trait Features {
  // The label column should be called "label", and
  //    feature dense vector "features"

  def assemblerStages : Array[PipelineStage]
}

