package io.acalio.tasks
import java.{util => ju}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import io.acalio.utility.Keys
import io.acalio.nlp.SentimentAnalyzer
import org.apache.parquet.format.DateType
import scala.collection.JavaConverters._

class  CommentView (
  override val name: String,
  override val inFormat: String,
  override val outFormat: String,
  override val applicationProperties: ju.Properties
) extends SparkTask(name, inFormat, outFormat, applicationProperties) {



  def execute() {
     // maybeInit()
    var commentDf: Dataset[Row] = null
    var streamingContext = false
    var awsReady = false
    inFormat match {
      case "kafka" => {
        var topic: String = applicationProperties.get(Keys.TOPIC).toString
        commentDf = readFromKafka()
        streamingContext = true
      }
      case "parquet" => {
        val commentPath = applicationProperties.get(CommentView.COMMENT_PATH).toString
        applicationProperties.put(
          Keys.SOURCE,
          commentPath)
        awsReady = maybeConfigureAws(commentPath)
        commentDf = readFromFileSystem()
      }
    }
    val result = commentView(commentDf)
    
    outFormat match {
      case "console" => {
        if(streamingContext)
          writeStreamToConsole(result, "/tmp/commentView")
        else
          result.show()
      }
      case "parquet" => {
        val path: String = applicationProperties.get(Keys.SINK).toString
        if(!awsReady)
          maybeConfigureAws(path)

        println(s"writing to $path")
        if(streamingContext)
          writeStreamToFileSystem(result, path, "/tmp/commentView")
        else
          writeStaticToFileSystem(result, path)
      }
    }

  }

  def commentView(commentDf: Dataset[Row]): Dataset[Row] = {
    import spark.implicits._
    //udf functions definition
    //get the length of each text
    val countWordsUDF = udf((text:String) => text.split(" ").length)
    val sentimentUDF = udf {
      // text: String =>  SentimentAnalyzer.sentiment(text)
      text: String => SentimentAnalyzer.getSentiment(text)
    }
    val sentimentClassUDF = udf {
      score: Double => {
        var sentClass: Int = 3 //positive
        if(score<=1)  sentClass = 0 //negative
        else if(score==2)  sentClass = 1 //neutral
        sentClass
      }
    }

    val result = commentDf
      .withColumn("sentiment", sentimentUDF(col("text")))
      .filter($"sentiment"<Double.MaxValue)
      .withColumn("textLength", countWordsUDF(col("text")))

    return result
  }

}

object CommentView {
  //application properties to be included into the Properties object
  val COMMENT_TOPIC: String = "threadview.comment.topic"
  val COMMENT_PATH: String = "threadview.comment.path"
}
















