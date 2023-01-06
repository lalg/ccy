package usd.util

import org.apache.logging.log4j.LogManager
import org.apache.log4j.Level
import org.apache.log4j.Logger

trait CcyLogging {
  val loggerName = this.getClass.getName()
  lazy val logger = LogManager.getLogger(loggerName)
  Logger.getLogger(loggerName).setLevel(Level.INFO)
}

// import org.slf4j.LoggerFactory
// trait CcyLogging {
//   def logger = LoggerFactory.getLogger(this.getClass)
// }

