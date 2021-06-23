package io.acalio.utility

import ch.qos.logback.classic.{Level,Logger}
import org.slf4j.LoggerFactory

object Utility {

  def setupLogging() {
      LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.ERROR)
  }
}
