package io.acalio.tasks

import java.{util => ju}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import io.acalio.utility.Keys


class VideoView (
  override val name: String,
  override val inFormat: String,
  override val outFormat: String,
  override val applicationProperties: ju.Properties
) extends SparkTask(name, inFormat, outFormat, applicationProperties) {


  def execute(){
    var videoDf: Dataset[Row] = null
    var commentDf: Dataset[Row] = null
    var streamingContext = false
    var awsReady = false

    inFormat match {
      case "kafka" => {
        var topic: String = applicationProperties.get(VideoView.VIDEO_TOPIC).toString
        applicationProperties.put(Keys.TOPIC, topic)
        videoDf = readFromKafka()

        topic = applicationProperties.get(VideoView.COMMENT_TOPIC).toString
        applicationProperties.put(Keys.TOPIC, topic)
        commentDf = readFromKafka()

        streamingContext = true
      }
      case "parquet" => {
        val videoPath = applicationProperties.get(VideoView.VIDEO_PATH).toString
        val commentPath = applicationProperties.get(VideoView.COMMENT_PATH).toString
        awsReady = maybeConfigureAws(videoPath)
        if(!awsReady)
          awsReady = maybeConfigureAws(commentPath)
        applicationProperties.put(Keys.SOURCE, videoPath)
        videoDf = readFromFileSystem()

        applicationProperties.put(Keys.SOURCE, commentPath)
        commentDf = readFromFileSystem()
      }
    }

    val result = getVideoRepresentation(videoDf,  commentDf)

    outFormat match {
      case "console" => {
        if(streamingContext)
          writeStreamToConsole(result, "/tmp/videoRepr")
        else
          result.show()
      }
      case "parquet" => {
        val path: String = applicationProperties.get(Keys.SINK).toString
        if(!awsReady)
          maybeConfigureAws(path)

        println(s"writing to $path")
        if(streamingContext)
          writeStreamToFileSystem(result, path, "/tmp/videoRepr")
        else
          writeStaticToFileSystem(result, path)
      }
    }

  }


  def getVideoRepresentation(videoDf: Dataset[Row], commentDf: Dataset[Row]) : Dataset[Row] = {
    //count the number of words
    val countWordsUDF = udf((text:String) => text.split(" ").length)
    val countElementsUDF = udf((x: Seq[String]) => x.length)
    //check if there is a link
    val thereIsLink = udf((x:String) =>  ".*(http[s])+.*".r.findAllIn(x).length>0)

    //extract only the top level comments, i.e., the starting point of a new thread
    import spark.implicits._
    val onlyTop = commentDf
      .filter($"parentID"===$"id")
      .groupBy(col("videoId"))                        //group by videoid
      .count()                                        //count the number of threads
      .withColumnRenamed("count", "threadCount")      //rename the count column

    // onlyTop.show()
    // videoDf.printSchema()
    // videoDf.show()
    val newVideoDf = videoDf
      .withColumn("link", thereIsLink(col("description")))          // check if the description contains any link
      .withColumn("description", countWordsUDF(col("description"))) // get description length
      .withColumn("title", countWordsUDF(col("title")))             // get title length
      .withColumn("tags", countElementsUDF(col("tags")))            // get tags length
      .as("v")                                                      //prepare for the join operation
      .join(onlyTop.as("c"), $"v.id"===$"c.videoId", "leftouter")   //join tih the comments statistics
      .drop("videoId")                                              //drop the column from the join operation
      .na.fill(Map("threadCount" -> 0))                             //replace null values with 0
      .withColumn("ct-ratio",                                       //compute the reation #comments/#threads
        when(col("threadCount")>0,col("threadCount")/col("commentCount")) 
        .otherwise(0))

    return newVideoDf

  }


}


object VideoView {
  lazy val VIDEO_TOPIC: String = "videoview.video.topic"
  lazy val VIDEO_PATH: String = "videoview.video.path"

  lazy val COMMENT_TOPIC: String = "videoview.comment.topic"
  lazy val COMMENT_PATH: String = "videoview.comment.path"

}






