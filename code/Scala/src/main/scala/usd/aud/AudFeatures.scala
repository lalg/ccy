package usd.aud

import usd.models.Features
import usd.models.CcyFeature
import usd.models.LogisticRegressionParams
import usd.util.CcyLogging
import usd.data.io.CcyDesignMatrixIo

trait AudFeatures
    extends Features
    with LogisticRegressionParams
    with CcyLogging {

  val features : List[CcyFeature]
  val designMatrixIo : CcyDesignMatrixIo

}

