package usd.data.io

import usd.util.CcyLogging
import org.apache.spark.ml.PipelineModel

class PipelineModelIo(val bucketPath : String)
    extends CcyLogging {

  def read : PipelineModel = {
    logger.info(s"${bucketPath} - model read")
    PipelineModel.load(bucketPath)
  }

  def write(pipelineModel : PipelineModel) = {
    logger.info(s"${bucketPath} - model write")
    pipelineModel
      .write
      .overwrite()
      .save(bucketPath)
  }
}

