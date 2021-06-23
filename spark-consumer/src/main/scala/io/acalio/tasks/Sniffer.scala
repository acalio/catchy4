package io.acalio.tasks

import io.acalio.utility.Keys
import java.{util => ju}
import org.apache.spark.sql.SparkSession
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.functions.from_avro

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row


class Sniffer(
  override val name: String,
  override val outFormat: String,
  override val applicationProperties: ju.Properties
) extends SparkTask(name, "kafka", outFormat, applicationProperties) {

  def execute() {
    val topic: String = applicationProperties.get(Keys.TOPIC).toString
    println(s"sniff on: $topic")
    //override the topic into the application properties
    applicationProperties.put(Keys.TOPIC, topic)
    val entityDf: Dataset[Row] = readFromKafka()
    entityDf.printSchema()
    
    //output configuration
    if(outFormat=="console"){
      writeStreamToConsole(entityDf, s"/tmp/$topic")
    }else if(outFormat=="parquet"){
      val path: String = applicationProperties.get(Keys.SINK).toString
      maybeConfigureAws(path)
      println(s"writing to $path")
      writeStreamToFileSystem(entityDf, path, s"/tmp/$topic")
    }
  }
}















