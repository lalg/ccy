package usd.apps

import org.apache.spark.sql.SparkSession
import usd.data.config.CcyEnv
import usd.data.config.{CcyPairTables, ElementalTables}
import usd.modeling.CcyModel



object CcyPairApp {
  def main(args : Array[String]) : Unit = {
    implicit val spark =
      SparkSession.builder()
        .appName("ccyPair")
        .getOrCreate()
    app(args)
  }

  def app(args : Array[String])(implicit
    spark:SparkSession) : Unit = {
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
