package usd.data.config

import org.apache.spark.sql.SparkSession


class CcyEnv(val envName: String) (implicit val spark: SparkSession)
    extends Serializable {

  implicit val env : Env = envName match {
    case "prod" => ProdEnv
    case "dev" => DevEnv
    case _ => throw new Exception(s"${envName} -- not recognized")
  }
}

