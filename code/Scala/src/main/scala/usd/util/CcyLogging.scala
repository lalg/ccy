package usd.util

import org.apache.logging.log4j.LogManager
import org.apache.log4j.Level
import org.apache.log4j.Logger

trait CcyLogging {
  val loggerName = this.getClass.getName()
  println(loggerName)
  lazy val logger = LogManager.getLogger("usd")
  Logger.getLogger("usd").setLevel(Level.INFO)
  println(logger.getLevel())
}

// import org.slf4j.LoggerFactory
// trait CcyLogging {
//   def logger = LoggerFactory.getLogger(this.getClass)
// }

