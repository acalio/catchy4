package io.acalio.ytproducers


import io.acalio.ytproducers.utility.{Utility, Keys}
import io.acalio.ytproducers.tasks._
import org.rogach.scallop._
import java.{util => ju}
import org.apache.kafka.common.serialization.StringSerializer
import redis.clients.jedis.HostAndPort
import io.acalio.ytproducers.utility.KeyManager
import scala.collection.mutable
import io.acalio.ytproducers.utility.DevKey
import org.acalio.dm.api.YoutubeDataManager


class Conf(args: Seq[String]) extends ScallopConf(args) {
  //limit to the the number of video to be processed
  trait VideoLimit {_ : ScallopConf =>
    lazy val videoLimit = opt[Long] ("video.limit", default=Some(Long.MaxValue))
  }

  //limit to the number of comment to be processed
  trait CommentLimit {_ : ScallopConf =>
    lazy val threadLimit = opt[Long] ("thread.limit", default=Some(Long.MaxValue))
  }

  //limit to the number of subscriptions to be processed
  trait SubscriptionLimit {_ : ScallopConf =>
    lazy val subscriptionLimit = opt[Long] ("sub.limit", default=Some(Long.MaxValue))
  }

  //lmit to the number of like to be processed
  trait LikeLimit {_ : ScallopConf =>
    lazy val likeLimit = opt[Long] ("like.limit", default=Some(Long.MaxValue))
  }

  //query to be executed
  trait Query {_ : ScallopConf =>
    lazy val query =  opt[String]("query", required=true)
  }

  //developer key
  lazy val devKeys = opt[List[String]]("dev.keys", required=true)

  //parameters to configure redis as cache
  lazy val redisProps = opt[List[String]]("redis.host", required = false)


  //**************************
  // list of subcommands
  //*************************

  //get the comment associated with a video
  lazy val commandComments = new Subcommand("comments") with CommentLimit {
    lazy val videoId = opt[String]("videoId", required=true)
  }

  //get the subscriptions related to a channel id
  lazy val commandSubscriptions = new Subcommand("subscriptions") with SubscriptionLimit {
    lazy val channelId = opt[String]("channelId", required=true)
  }
  //get the information related to a channel id
  lazy val commandChannel = new Subcommand("channel") {
    lazy val channelId = opt[String]("channelId", required=true)
  }
  //get the likes of a user
  lazy val commandLikes = new Subcommand("likes") with LikeLimit {
    lazy val channelId = opt[String]("channelId", required=true)
  }
  //get information about a video
  lazy val commandVideo = new Subcommand("video") with CommentLimit {
    lazy val videoId = opt[String]("videoId", required=true)
  }


  lazy val commandPipeline = new Subcommand("pipeline")
      with Query with CommentLimit with SubscriptionLimit with LikeLimit with VideoLimit  {
  }

  lazy val limit = opt[Int]("max-results", default=Some(10))
  lazy val frequency = opt[Long]("frequency", default=Some(500))
  lazy val topic = opt[String]("topic", default=Some("mytopic"))
  lazy val broker = opt[String]("broker", default=Some("127.0.0.1:9092"))
  lazy val registry = opt[String]("registry", default=Some("http://localhost:8081"))

  //add subcommand for executing a query
  addSubcommand(commandComments)
  addSubcommand(commandSubscriptions)
  addSubcommand(commandChannel)
  addSubcommand(commandLikes)
  addSubcommand(commandVideo)
  addSubcommand(commandPipeline)

  verify

}

object Producer  {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    Utility.setupLogging()
    val kafkaProducerProps: ju.Properties = {
      val props = new ju.Properties()
      props.put(Keys.BROKER_URL, conf.broker.getOrElse(""))
      props.put(Keys.KEY_SERIALIZER, classOf[StringSerializer].getName())
      props.put(Keys.VALUE_SERIALIZER, "io.confluent.kafka.serializers.KafkaAvroSerializer")
      props.put(Keys.SCHEMA_REGISTRY_URL, conf.registry.getOrElse(""))
      props
   }

    var task: KafkaTask = null
    var config = new ju.Properties
    config.put(Keys.TOPIC_NAME, conf.topic.getOrElse(""))

    //get the developer keys
    val keysString = conf.devKeys.get.get
    val keys = new mutable.ArrayBuffer[DevKey]
    for(k <- keysString) keys += new DevKey(k)
    val manager = new KeyManager(keys)
    //set the initial developer key to the YT data manager
    YoutubeDataManager.setDeveloperKey(keysString(0))


    //execute the command
    val subCommand = conf.subcommand
    subCommand  match {
      case Some(conf.commandPipeline) => {
        config.put(Keys.LIMIT_COMMENT, conf.commandPipeline.threadLimit.getOrElse(Long.MaxValue).asInstanceOf[java.lang.Long])
        config.put(Keys.LIMIT_VIDEO, conf.commandPipeline.videoLimit.getOrElse(Long.MaxValue).asInstanceOf[java.lang.Long])
        config.put(Keys.LIMIT_LIKE, conf.commandPipeline.likeLimit.getOrElse(Long.MaxValue).asInstanceOf[java.lang.Long])
        config.put(Keys.LIMIT_SUBSCRIPTION, conf.commandPipeline.subscriptionLimit.getOrElse(Long.MaxValue).asInstanceOf[java.lang.Long])
        config.put(Keys.QUERY, conf.commandPipeline.query.getOrElse(""))
        task = new FullPipelineProducer(manager, kafkaProducerProps, config)
      }

      case Some(conf.commandChannel) => {
        config.put(Keys.CHANNEL_ID, conf.commandChannel.channelId.getOrElse(""))
        task = new ChannelProducer(manager, kafkaProducerProps, config)
      }

      case Some(conf.commandComments) => {
        config.put(Keys.VIDEO_ID, conf.commandComments.videoId)
        config.put(Keys.LIMIT_VIDEO, conf.commandComments.threadLimit.getOrElse(Long.MaxValue).asInstanceOf[java.lang.Long])
        task = new CommentProducer(manager, kafkaProducerProps, config)
      }

      case Some(conf.commandLikes) => {
        config.put(Keys.CHANNEL_ID, conf.commandLikes.channelId.getOrElse(""))
        config.put(Keys.LIMIT_LIKE, conf.commandLikes.likeLimit.getOrElse(Long.MaxValue).asInstanceOf[java.lang.Long])
        task = new LikeProducer(manager, kafkaProducerProps, config)
      }

      case Some(conf.commandSubscriptions) => {
        config.put(Keys.CHANNEL_ID, conf.commandSubscriptions.channelId.getOrElse(""))
        config.put(Keys.LIMIT_SUBSCRIPTION, conf.commandSubscriptions.subscriptionLimit.getOrElse(Long.MaxValue).asInstanceOf[java.lang.Long])
        task = new SubscriptionProducer(manager, kafkaProducerProps, config)
      }

      case Some(conf.commandVideo) => {
        config.put(Keys.VIDEO_ID, conf.commandVideo.videoId.getOrElse(""))
        task = new VideoProducer(manager, kafkaProducerProps, config)

      }
      case _ => println("Unrecognized option")
    }


    //configure redis
    conf.redisProps.get match {
      case None => {}
      case Some(value) => {
        var hosts = new ju.HashSet[HostAndPort]
        for(h <- value){
          val splitted =h.split(':')
          hosts.add(new HostAndPort(splitted(0), splitted(1).toInt))
        }
        task.setHistory(new io.acalio.ytproducers.utility.History(hosts))
      }
    }

    task.execute()
}

}



