package usd.data.config

import org.apache.spark.sql.SparkSession

// spark session required by sub-classes
class CcyEnv(val envName: String) (implicit val spark: SparkSession)
    extends Serializable {

  // this does not need to be an implicit
  implicit val env : Env = envName match {
    case "prod" => ProdEnv
    case "dev" => DevEnv
    case _ => throw new Exception(s"${envName} -- not recognized")
  }
}

