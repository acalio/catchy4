package io.acalio.tasks

import java.{util => ju}
import za.co.absa.abris.config.FromAvroConfig
import za.co.absa.abris.config.AbrisConfig
import io.acalio.utility.Keys
import za.co.absa.abris.config.AbrisConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.abris.avro.functions.from_avro

abstract class  SparkTask(
  val name: String,
  val inFormat: String,
  val outFormat: String,
  val applicationProperties: ju.Properties
) {

    
  val spark = SparkSession.builder
      .appName(name)
       .master("local[*]")
       .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .getOrCreate()

  def execute() 


  protected def readFromKafka(): Dataset[Row] = {
    val topic = applicationProperties.get(Keys.TOPIC).toString
    val df = spark.readStream
      .format("kafka")
      .option("subscribe", topic) //the topic must point to a specific entity
      .option(Keys.BROKER_URL, applicationProperties.get(Keys.BROKER_URL).toString)
      .option(Keys.OFFSET, applicationProperties.get(Keys.OFFSET).toString)
      .option("failOnDataLoss", "false")
      .load()

    val abrisConfig = configureAvro(topic)

    val entityDf = df.select(from_avro(col("value"), abrisConfig) as 'entity)
      .select("entity.*")
    return entityDf
  }


  def readFromFileSystem(): Dataset[Row]  = {
    //set configuration for aws
      val sourcePath: String  = s"${applicationProperties.get(Keys.SOURCE).toString()}"
      println(s"Reading from: $sourcePath")
      val df = spark
        .read
        .parquet(sourcePath)

    return df
  }


  protected def writeStreamToFileSystem(df: Dataset[Row], awsPath: String, checkPoint: String) {
//    configureAws() 
    df.writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", awsPath)
      .option("checkpointLocation",checkPoint)
      .start()
      .awaitTermination()
  }

  def writeStaticToFileSystem(df: Dataset[Row], awsPath: String) {
    df.write.parquet(awsPath)
  }

  def writeStreamToConsole(df: Dataset[Row], checkPoint: String) {
    df.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", checkPoint)
      .start()
      .awaitTermination()
  }



  protected def configureAvro(topic: String): FromAvroConfig = {
    return AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(topic)
      .usingSchemaRegistry(applicationProperties.get(Keys.SCHEMA_REGISTRY_URL).toString)
  }


  protected def maybeConfigureAws(path: String): Boolean = {
    if(path.startsWith("s3")){
      configureAws()
      return true
    }
    return false
  }

  protected def configureAws()  {
    //write stream to aws
    spark.sparkContext.hadoopConfiguration.set(
      Keys.S3_SECRET_KEY, applicationProperties.get(Keys.S3_SECRET_KEY).toString)

    spark.sparkContext.hadoopConfiguration.set(
      Keys.S3_ACCESS_KEY, applicationProperties.get(Keys.S3_ACCESS_KEY).toString)

    spark.sparkContext.hadoopConfiguration.set(
      Keys.S3_ENDPOINT, applicationProperties.get(Keys.S3_ENDPOINT).toString)
  }

}
