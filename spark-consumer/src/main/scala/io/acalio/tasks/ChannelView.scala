package io.acalio.tasks

import java.{util => ju}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import io.acalio.utility.Keys


class ChannelView (
  override val name: String,
  override val inFormat: String,
  override val outFormat: String,
  override val applicationProperties: ju.Properties
) extends SparkTask(name, inFormat, outFormat, applicationProperties) {



  def execute(){
    var channelDf: Dataset[Row] = null
    var subscriptionDf: Dataset[Row] = null
    var streamingContext = false
    var awsReady = false
    inFormat match {
      case "kafka" => {
        var topic: String = applicationProperties.get(ChannelView.CHANNEL_TOPIC).toString
        applicationProperties.put(Keys.TOPIC, topic)
        channelDf = readFromKafka()

        topic = applicationProperties.get(ChannelView.SUBSCRIPTION_TOPIC).toString
        applicationProperties.put(Keys.TOPIC, topic)
        subscriptionDf = readFromKafka()
      }  
      case "parquet" => {
        val channelPath = applicationProperties.get(ChannelView.CHANNEL_PATH).toString
        val subPath = applicationProperties.get(ChannelView.SUBSCRIPTION_PATH).toString
        println(s"Reading: $channelPath, $subPath")
        awsReady = maybeConfigureAws(channelPath)
        if(!awsReady)
          awsReady = maybeConfigureAws(subPath)

        //read channels
        applicationProperties.put(Keys.SOURCE, channelPath)
        channelDf = readFromFileSystem()

        //read subscriptions
        applicationProperties.put(Keys.SOURCE, subPath)
        subscriptionDf = readFromFileSystem()
      }
    }

    val result = getChannelView(channelDf, subscriptionDf)
    outFormat match {
      case "console" => {
        if(streamingContext)
          writeStreamToConsole(result, "/tmp/channelView")
        else
          result.show()
      }
      case "parquet" => {
        val path: String =  applicationProperties.get(Keys.SINK).toString
        if(!awsReady)
          maybeConfigureAws(path)

        println(s"writing to $path")
        if(streamingContext)
          writeStreamToFileSystem(result, path, "/tmp/channelView")
        else
          writeStaticToFileSystem(result, path)
      }
    }
  }

  def getChannelView(channelDf: Dataset[Row], subscriptionDf: Dataset[Row]):  Dataset[Row] = {
        import spark.implicits._
    //udf functions definition
    val countWordsUDF = udf((text:String) => text.split(" ").length)

    val followeeDf = subscriptionDf
      .groupBy(col("subscriberId"))
      .count()
      .withColumnRenamed("count", "followeeCount")

    val result = channelDf
      .withColumn("description", countWordsUDF(col("description")))
      .withColumn("title", countWordsUDF(col("title")))
      .withColumn("country",
        when(col("country").isNull, false).otherwise(true))
      .join(followeeDf, $"subscriberId"===$"id", "leftouter")
      .drop("subscriberId")
      .na.fill(Map("followeeCount" -> 0))
      .withColumn("sf-ratio",
        when(col("followeeCount")>0, col("subscriberCount")/col("followeeCount"))
          .otherwise(0))
    return result
  }
  
}

object ChannelView {
  //application properties to be included into the Properties object
  val CHANNEL_TOPIC: String = "channelview.channel.topic"
  val CHANNEL_PATH: String = "channelview.channel.path"
  val SUBSCRIPTION_TOPIC: String = "channelview.subscription.topic"
  val SUBSCRIPTION_PATH: String = "channelview.subscription.path"
}




















