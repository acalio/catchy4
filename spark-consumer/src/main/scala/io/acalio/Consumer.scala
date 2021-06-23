package io.acalio
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.config.AbrisConfig
import ch.qos.logback.classic.{Level,Logger}
import org.slf4j.LoggerFactory
import java.nio.file.{Files, Paths}
import org.rogach.scallop._
import java.{util => ju}
import io.acalio.utility.Utility._
import io.acalio.utility.Keys
// import io.acalio.utility.sentiment._
import io.acalio.tasks._


class Conf(args: Seq[String]) extends ScallopConf(args) {

  //Collection of datatraits - a trait for each entity
  trait ChannelTrait { _ : ScallopConf => 
    lazy val chTopic = opt[String]("channel-topic", default=Some("channel"))
    lazy val chPath = opt[String]("channel-path", default=Some("ignore"))
  }

  trait VideoTrait { _ : ScallopConf =>
    lazy val viTopic = opt[String]("video-topic", default=Some("video"))
    lazy val viPath = opt[String]("video-path", default=Some("ignore"))
  }

  trait SubscriptionTrait {_ : ScallopConf => 
    lazy val subTopic = opt[String]("sub-topic", default = Some("subscription"))
    lazy val subPath = opt[String]("sub-path", default = Some("ignore"))
  }

  trait LikeTrait {_ : ScallopConf => 
    lazy val likeTopic = opt[String]("like-topic", default = Some("like"))
    lazy val likePath = opt[String]("like-path", default = Some("ignore"))
  }

  trait CommentTrait {_ : ScallopConf => 
    lazy val commentTopic = opt[String]("comment-topic", default = Some("comment"))
    lazy val commentPath = opt[String]("comment-path", default = Some("ignore"))
  }

  // trait Formats
  lazy val inFormat = opt[String]("in-format", default = Some("kafka"), descr="type of the stream")
  lazy val outFormat = opt[String]("out-format", default = Some("console"), descr="type of the outputstream")

  // trait KafkaArguments 
  lazy val topic = opt[String]("topic", default=Some("mytopic"))
  lazy val broker = opt[String]("broker", default=Some("127.0.0.1:9092"))
  lazy val registry = opt[String]("registry", default=Some("http://localhost:8081"))
  lazy val offset = opt[String]("offset", default = Some("earliest"))

  // trait AWSArguments
  lazy val accessKey = opt[String]("access-key", default=Some("<your-key>"))
  lazy val secretKey = opt[String]("secret-key", default=Some("<your-secret>"))
  lazy val endpoint = opt[String]("aws-endpoint", default=Some("s3.amazonaws.com"))
//  lazy val sourceBasePath = opt[String]("aws-src-base", default=Some("<your base>"))
  //lazy val sinkBasePath = opt[String]("aws-sink-base", default=Some("<your base>"))

  // //file system location arguments
//  lazy val source = opt[String]("src", default = Some("ignore")) // 
  lazy val sink = opt[String]("sink", default=Some("ignore"))

  //read the comments and generate the interaction graph
  lazy val interactionGraph = new Subcommand("interaction") with CommentTrait

  //read from every available topic an dave to aws
  lazy val sniffEntity = new Subcommand("sniff")


  //get the video vector representation
  lazy val videoView = new Subcommand("video-view") with VideoTrait with CommentTrait

  //get the channel representation
  lazy val channelView = new Subcommand("channel-view")  with ChannelTrait with  SubscriptionTrait

  //get the thread representation
  lazy val threadView = new Subcommand("thread-view") with CommentTrait

  //get the comment representation
  lazy val commentView = new Subcommand("comment-view") with CommentTrait


  //add subcommand to the consumer
  addSubcommand(interactionGraph)
  addSubcommand(sniffEntity)
  addSubcommand(videoView)
  addSubcommand(channelView)
  addSubcommand(threadView)
  addSubcommand(commentView)

  verify
}

object Consumer  {

  case class KeyValuePair(key: String, value: String)

  def setProps(appProperties: ju.Properties, kvPairs: KeyValuePair*)  {
    kvPairs.foreach( x=> appProperties.put(x.key,x.value))
  }

  def main(args: Array[String]) = {
    setupLogging()
    val conf = new Conf(args)
    //configure comment arguments
    val appProperties = new ju.Properties;
    val inFormat = conf.inFormat.getOrElse("")
    val outFormat = conf.outFormat.getOrElse("")

    // //check if the user wants to write on s3
    // if (sourcePath.startsWith("s3a") || sinkPath.startsWith("s3a")) {
    //   println("setting aws")
    //   setProps(appProperties,
    //     KeyValuePair(Keys.S3_SECRET_KEY, conf.secretKey.getOrElse("")),
    //     KeyValuePair(Keys.S3_ACCESS_KEY, conf.accessKey.getOrElse("")),
    //     KeyValuePair(Keys.S3_ENDPOINT, conf.endpoint.getOrElse("")))
    // }

    if(inFormat=="kafka"){
      setProps(appProperties,
        KeyValuePair(Keys.OFFSET, conf.offset.getOrElse("")),
        KeyValuePair(Keys.TOPIC, conf.topic.getOrElse("")), 
        KeyValuePair(Keys.BROKER_URL, conf.broker.getOrElse("")),
        KeyValuePair(Keys.SCHEMA_REGISTRY_URL,conf.registry.getOrElse("")))
    }
    // else if(inFormat=="parquet") {
    //   setProps(appProperties,
    //     // KeyValuePair(Keys.S3_SOURCE_BASE, conf.sourceBasePath.getOrElse("")),
    //     // KeyValuePair(Keys.S3_SOURCE, conf.awsSource.getOrElse("")))
    //     KeyValuePair(Keys.SOURCE, sourcePath))
    // }

    //set the  shink path
    if(outFormat=="parquet") {
      setProps(appProperties,
        // KeyValuePair(Keys.S3_SINK_BASE, conf.sinkBasePath.getOrElse("")),
        // KeyValuePair(Keys.S3_SINK, conf.awsSink.getOrElse("")))
        KeyValuePair(Keys.SINK, conf.sink.getOrElse("")))
    }

    //get the actual subcommand
    val subCommand = conf.subcommand
    var task: SparkTask = null
    subCommand match {
      case Some(conf.sniffEntity ) => {
        task  = new Sniffer("sniffer", outFormat, appProperties)

      }
      case Some(conf.interactionGraph) => {
        setProps(appProperties,
          KeyValuePair(InteractionGraph.COMMENT_TOPIC, conf.interactionGraph.commentTopic.getOrElse("")),
          KeyValuePair(InteractionGraph.COMMENT_PATH, conf.interactionGraph.commentPath.getOrElse("")),
        )
        task = new InteractionGraph("ig", inFormat, outFormat, appProperties)
        
      }
      case Some(conf.videoView) => {
        setProps(appProperties,
          KeyValuePair(VideoView.VIDEO_TOPIC, conf.videoView.viTopic.getOrElse("")),
          KeyValuePair(VideoView.VIDEO_PATH, conf.videoView.viPath.getOrElse("")),
          KeyValuePair(VideoView.COMMENT_TOPIC, conf.videoView.commentTopic.getOrElse("")),
          KeyValuePair(VideoView.COMMENT_PATH, conf.videoView.commentPath.getOrElse(""))
        )
        task = new VideoView("video", inFormat, outFormat, appProperties)        
      }
      case Some(conf.channelView) => {
        setProps(appProperties,
          KeyValuePair(ChannelView.CHANNEL_TOPIC, conf.channelView.chTopic.getOrElse("")),
          KeyValuePair(ChannelView.CHANNEL_PATH, conf.channelView.chPath.getOrElse("")),
          KeyValuePair(ChannelView.SUBSCRIPTION_TOPIC, conf.channelView.subTopic.getOrElse("")),
          KeyValuePair(ChannelView.SUBSCRIPTION_PATH, conf.channelView.subPath.getOrElse(""))
        )
        task = new ChannelView("channel", inFormat, outFormat, appProperties)
      }
      case Some(conf.threadView) => {
        setProps(appProperties,
          KeyValuePair(ThreadView.COMMENT_TOPIC, conf.threadView.commentTopic.getOrElse("")),
          KeyValuePair(ThreadView.COMMENT_PATH, conf.threadView.commentPath.getOrElse(""))
        )
        // val lan = conf.threadView.language.getOrElse("")
        // SentimentAnalyzer.setLanguage(Language.get(lan)) // set the default language
        task = new ThreadView("thread", inFormat, outFormat, appProperties)
      }
      case Some(conf.commentView) =>  {
        setProps(appProperties,
          KeyValuePair(CommentView.COMMENT_TOPIC, conf.commentView.commentTopic.getOrElse("")), 
          KeyValuePair(CommentView.COMMENT_PATH, conf.commentView.commentPath.getOrElse(""))
        )
        task = new CommentView("cview", inFormat, outFormat, appProperties)
      }
      case _ => println("unrecognized option")
    }
    task.execute()
  }

}








