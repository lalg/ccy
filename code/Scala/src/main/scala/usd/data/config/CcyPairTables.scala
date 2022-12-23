package usd.data.config

import usd.data.io.PipelineModelIo
import usd.data.source.CurrencyPairs
import usd.data.config.CcyEnv

trait CcyPairTables extends CcyEnv {
  def ccyPairModel(modelName: String, ccyPair: CurrencyPairs.CcyPair) = {
    val modelRoot = s"${env.hdfsRoot}/${env.envName}/modelName"
    new PipelineModelIo(s"${modelRoot}/${ccyPair}")
  }
}

