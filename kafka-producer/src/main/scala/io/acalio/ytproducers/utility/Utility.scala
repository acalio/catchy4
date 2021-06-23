package io.acalio.ytproducers.utility

import java.util.Properties
import java.io.InputStream
import ch.qos.logback.classic.{Level,Logger}
import org.slf4j.LoggerFactory

object Utility {

  def getApplicationProps(): Properties = {
    val stream: InputStream = getClass.getResourceAsStream("/application.properties")
    val prop = new Properties()
    prop.load(stream)
    return prop
  }


  def getKafkaProducerProperties(): Properties = {
    val stream: InputStream = getClass.getResourceAsStream("/producer.properties")
    val prop = new Properties()
    prop.load(stream)
    return prop
  }

  def setupLogging() {
      LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.ERROR)
  }

}
