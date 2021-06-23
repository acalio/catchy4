package io.acalio.tasks
import java.{util => ju}
import org.apache.spark.sql.SparkSession
import io.acalio.utility.Keys
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.abris.avro.functions.from_avro
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.DataStreamWriter
// import io.acalio.utility.sentiment.SentimentAnalyzer
import scala.collection.JavaConverters._
import io.acalio.nlp.SentimentAnalyzer 
/**
  * Class for extracting the interaction graph 
  * from the comment threads
  * 
  * @constructor create a new InteractionGraph
  * @param name the name of the spark context
  * @param inFormat where to read data. 
  *        if "kafka" comments are read from the kafka topic
  *        if "parquet" they are stored in parquet inFormat on aws
  * @param properties to initialize the task
  */
class InteractionGraph(
  override val name: String,
  override val inFormat: String, // inFormat = kafka if you want to read from kafka,
  override val outFormat: String,
  override val applicationProperties: ju.Properties,
) extends SparkTask(name, inFormat, outFormat, applicationProperties) {

  def execute() {
    var commentsDf: Dataset[Row] = null
    var streamingContext: Boolean = false
    var awsIsReady = false
    inFormat match {
      case "kafka" => {
        applicationProperties.setProperty(
          Keys.TOPIC,
          applicationProperties.get(InteractionGraph.COMMENT_TOPIC).toString
        )
        commentsDf = readFromKafka()
        streamingContext = true
      }
      case "parquet" => {
        val commentPath = applicationProperties.get(InteractionGraph.COMMENT_PATH).toString
        applicationProperties.put(
          Keys.SOURCE,
          commentPath)
        awsIsReady = maybeConfigureAws(commentPath)
        commentsDf = readFromFileSystem()
      }
    }

//    commentsDf.printSchema()
    val interactionDf = extractInteractionGraph(commentsDf)
    outFormat match {
      case "console" => {
        if(streamingContext)
          writeStreamToConsole(interactionDf, "/tmp/interaction")
        else
          interactionDf.show()
      }
      case "parquet" => {
        val path: String = applicationProperties.get(Keys.SINK).toString
        if(!awsIsReady)
          maybeConfigureAws(path)

        if(streamingContext)
          writeStreamToFileSystem(interactionDf, path, "/tmp/interaction" )
        else
          writeStaticToFileSystem(interactionDf, path)
      }
    }
  }

  private def extractInteractionGraph(commentsDf : Dataset[Row]): Dataset[Row] = {
    import spark.implicits._
    val sentimentUDF = udf {
      //text: String =>  SentimentAnalyzer.sentiment(text).getOrElse(Double.MinValue)
      text: String => SentimentAnalyzer.getSentiment(text)
    }
    val interactionDf = commentsDf
      .as("left")
      .join(commentsDf.as("right"), $"right.parentID" === $"left.id")
      .select(
        col("left.authorChannelId").as("parent"), 
        col("right.authorChannelId").as("child"),
        col("right.text")).persist()

    val result = interactionDf.withColumn("textScore", sentimentUDF($"text"))
      .withColumn("textScore", sentimentUDF($"text"))

    return result
  }

}

object InteractionGraph {
  //application properties to be included into the Properties object
  val COMMENT_TOPIC: String = "interactiongraph.comment.topic"
  val COMMENT_PATH: String = "interactiongraph.comment.path"
}










