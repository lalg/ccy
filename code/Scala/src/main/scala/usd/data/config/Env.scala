package usd.data.config

trait Env {
  def envName : String
  def hdfsRoot : String
}

case object ProdEnv extends Env {
  def envName = "prod"
  def hdfsRoot = "hdfs:///ccy/usd/prod"
}

case object DevEnv extends Env {
  def envName = "dev"
  def hdfsRoot = "hdfs:///ccy/usd/dev"
}

