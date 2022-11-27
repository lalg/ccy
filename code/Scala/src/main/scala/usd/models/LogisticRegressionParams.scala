package usd.models

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.ParamMap

trait LogisticRegressionParams
    extends Params {


  val maxIterationParam =
    new Param[Int]("ccy", "maxIteration", "max number of iterations on SGD")
  val fitInterceptParam =
    new Param[Boolean]("ccy", "fitIntercept", "true for fitting intercept")

  val params =
    ParamMap(
      maxIterationParam -> 100,
      fitInterceptParam -> true)

  def setMaxIteration(v: Int) = setParam(maxIterationParam, v)
  def setFitIntercept(b: Boolean) = setParam(fitInterceptParam, b)

  def getMaxIteration = getParam(maxIterationParam)
  def getFitIntercept = getParam(fitInterceptParam)  
}



