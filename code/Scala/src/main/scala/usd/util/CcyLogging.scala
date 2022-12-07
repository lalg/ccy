package usd.util

import org.apache.logging.log4j.LogManager
trait CcyLogging {
  val loggerName = this.getClass.getName()
  lazy val logger = LogManager.getLogger(loggerName)
}

// import org.slf4j.LoggerFactory
// trait CcyLogging {
//   def logger = LoggerFactory.getLogger(this.getClass)
// }
