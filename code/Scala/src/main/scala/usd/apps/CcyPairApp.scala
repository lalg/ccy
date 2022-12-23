package usd.apps

import org.rogach.scallop
import org.apache.spark.sql.SparkSession
import usd.util.DateUtils
import usd.data.config.CcyEnv
import usd.data.config.{CcyPairTables, ElementalTables}
import usd.modeling.CcyModel
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
  verify()

  def trainStartDate = DateUtils(trainFromDate()).dt
  def trainEndDate = DateUtils(trainToDate()).dt
  def testStartDate = DateUtils(testFromDate()).dt
  def testEndDate = DateUtils(testToDate()).dt
  def predictionDate = DateUtils(predictionDt()).dt
  def horizon = predictionHorizon.getOrElse(14)
  def ccyPair = CurrencyPairs.withName(currencyPair())
}


object CcyPairApp {
  def main(args : Array[String])(implicit
    spark:SparkSession) = {
    val conf = CcyPairConf(args.toSeq)

    implicit val env =
      new CcyEnv(conf.envName())
          with ElementalTables
          with CcyPairTables

    val ccyModel = CcyModel(conf: CcyPairConf)

    conf.process() match {
//      case "PREDICT" => ccyModel.predictAndSave()
      case "EVALUATE" => ccyModel.evaluate()
//      case "TRAIN" => ccyModel.deployFinal()
      case p =>
        throw new Exception(s"${p} --- not recognized")
    }
  }
}
