package usd.data.config


case class CcyEnv(envName: String)
    extends Env with Serializable {

  def hdfsRoot = s"hdfs:///ccy/usd/${envName}"
}


object CcyEnv {
  def apply(envName: String) : CcyEnv = envName match {
    case "dev" | "prod" => CcyEnv(envName)
    case _ =>
      throw new Exception(s"${envName} - not recognized")
  }
}
