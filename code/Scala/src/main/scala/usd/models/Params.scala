package usd.models

import org.apache.spark.ml.param.{ParamMap, Param}

trait Params {
  protected def params : ParamMap

  def setParam[T](param: Param[T], v:T) : this.type = {
    params.put(param, v)
    this
  }

  def getParam[T](param: Param[T]) = params.get(param).get
}


