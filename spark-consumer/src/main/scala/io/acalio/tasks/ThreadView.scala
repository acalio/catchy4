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

class  ThreadView (
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
        val commentPath = applicationProperties.get(ThreadView.COMMENT_PATH).toString
        applicationProperties.put(
          Keys.SOURCE,
          commentPath)
        awsReady = maybeConfigureAws(commentPath)
        commentDf = readFromFileSystem()
      }
    }
    val result = getThreadView(commentDf)
    
    outFormat match {
      case "console" => {
        if(streamingContext)
          writeStreamToConsole(result, "/tmp/threadView")
        else
          result.show()
      }
      case "parquet" => {
        val path: String = applicationProperties.get(Keys.SINK).toString
        if(!awsReady)
          maybeConfigureAws(path)

        println(s"writing to $path")
        if(streamingContext)
          writeStreamToFileSystem(result, path, "/tmp/threadView")
        else
          writeStaticToFileSystem(result, path)
      }
    }

  }

  // def maybeInit() {
  //   val version = "4.0.0"
  //   val baseUrl = s"https://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp"
  //   val model = s"stanford-corenlp-$version-models.jar" //
  //   val url = s"$baseUrl/$version/$model"
  //   if (!spark.sparkContext.listJars().exists(jar => jar.contains(model))) {
  //     import scala.sys.process._
  //     // download model
  //     s"wget -N $url".!!
  //     // make model files available to driver
  //     s"jar xf $model".!!
  //     // add model to workers
  //     spark.sparkContext.addJar(model)
  //   } else {
  //     print("coreNLP models - OK!")
  //   }
  // }


  def getThreadView(commentDf: Dataset[Row]): Dataset[Row] = {
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

    val dayViewDf = commentDf
      .withColumn("date", date_format(from_unixtime($"publishedAt"/1000),"yyyy-MM-dd"))  //divide because they are millis
      .rollup($"parentID", $"date")
      .agg(
        countDistinct("id").as("replies"), //no. of comments
      )
      .groupBy($"parentID")
      .agg(
        sum("replies").as("totalReplies"),
        avg("replies").as("avgRepliesPerDay"),
        min("replies").as("minRepliesPerDay"),
        max("replies").as("maxRepliesPerDay"),
        stddev("replies").as("stdRepliesPerDay")
      )
      .withColumn("burstiness", ($"stdRepliesPerDay" - $"avgRepliesPerDay") / ($"stdRepliesPerDay" + $"avgRepliesPerDay")).persist()


    val commentStats = commentDf
      .withColumn("sentiment", sentimentUDF(col("text")))
      .filter($"sentiment"<Double.MaxValue)
      .withColumn("negative", when($"sentiment"<=1, 1).otherwise(0))
      .withColumn("neutral", when($"sentiment" === 2, 1).otherwise(0))
      .withColumn("positive", when($"sentiment">=3, 1).otherwise(0))
      .withColumn("text", countWordsUDF(col("text")))
      .groupBy($"parentID")
       .agg(
         min("publishedAt").as("firstAt"), 
         max("publishedAt").as("lastAt"),

         min("text").as("mintext"),
         max("text").as("maxtext"),
         avg("text").as("avgtext"),
         stddev("text").as("stdtext"),

         min("likeCount").as("minLikeCount"),
         max("likeCount").as("maxLikeCount"),
         avg("likeCount").as("avgLikeCount"),
         stddev("likeCount").as("stdLikeCount"),

         sum("positive").as("positiveCount"),
         sum("neutral").as("neutralCount"),
         sum("negative").as("negativeCount"),

         min("sentiment").as("minSentiment"),
         max("sentiment").as("maxSentiment"),
         avg("sentiment").as("avgSentiment"),
         stddev("sentiment").as("stdSentiment")

       )
      .withColumn("duration", $"lastAt"-$"firstAt").persist()
    val result = commentStats
      .join(dayViewDf, "parentID")
      .join(commentDf.select("parentID", "videoId", "authorChannelId"),"parentID") // qua introduco duplicati

    return result
  }

  // def sentiment = udf { text: String =>
  //   SentimentAnalyzer.sentiment(text)
  // }

}

object ThreadView {
  //application properties to be included into the Properties object
  val COMMENT_TOPIC: String = "threadview.comment.topic"
  val COMMENT_PATH: String = "threadview.comment.path"
}
















