package usd.apps

import usd.modeling.CcyModel

// evaluate a set of features individually
object CcyPairResearchApp {
  def apply(features : List[String]) : Unit  = {
    val args =
      Array(
        "--currency-pair", "AUDUSD",
        "--env-name", "dev",
        "--process", "EVALUATE",
        "--train-from-date", "2006-01-03",
        "--train-to-date", "2021-01-01",
        "--test-from-date", "2021-01-01",
        "--test-to-date", "2022-01-01")

    features foreach (f => CcyPairApp.main(args ++ Array(f)))
  }

  def apply(featureFile : String) : Unit = {
    val features =
      scala.io.Source.fromFile(featureFile).getLines().toList
    CcyPairResearchApp(features)
  }
}
