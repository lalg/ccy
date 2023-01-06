package usd.apps

import org.rogach.scallop
import usd.util.DateUtils
import usd.data.source.CurrencyPairs

case class CcyPairConf(arguments: Seq[String])
    extends scallop.ScallopConf(arguments) {

  val envName = opt[String](required = true)
  val currencyPair = opt[String](required = true)
  val process =
    choice(
      required=true,
      choices=Seq("TRAIN", "PREDICT","EVALUATE"))
  val trainFromDate = opt[String]()
  val trainToDate = opt[String]()
  val testFromDate  = opt[String]()
  val testToDate = opt[String]()
  val predictionDt = opt[String]()
  val predictionHorizon = opt[Int]()
  val select =
    trailArg[List[String]](required=false, default=Some(List.empty))

  verify()

  def trainStartDate = DateUtils(trainFromDate()).dt
  def trainEndDate = DateUtils(trainToDate()).dt
  def testStartDate = DateUtils(testFromDate()).dt
  def testEndDate = DateUtils(testToDate()).dt
  def predictionDate = DateUtils(predictionDt()).dt
  def horizon = predictionHorizon.getOrElse(14)
  def ccyPair = CurrencyPairs.withName(currencyPair())
  def selectedFeatures = select()
}


