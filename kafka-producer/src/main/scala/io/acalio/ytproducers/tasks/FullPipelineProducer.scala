package io.acalio.ytproducers.tasks

import io.acalio.ytproducers.utility._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.acalio.dm.api.YoutubeDataManager
import org.acalio.dm.model.avro.{YLike,YVideo, YComment, YChannel, YSubscription}
import java.{util => ju}
import util.control.Breaks._
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._
import com.google.api.client.googleapis.json.GoogleJsonResponseException


class FullPipelineProducer (
  override val keyManager: KeyManager,
  override val producerProperties: ju.Properties,
  val pipelineProperties: ju.Properties
) extends KafkaTask(keyManager, producerProperties) {


  def executeImpl() {
    //get the topic name
    val topic: String = pipelineProperties.get(Keys.TOPIC_NAME).asInstanceOf[String]
    //get the query
    val query = pipelineProperties.get(Keys.QUERY).toString

    //execute the query 
    println(s"Start crawling from youtube\nPublishing to: $topic \nQuery: $query")

    val relatedVideo: ju.List[YVideo] = YoutubeDataManager
      .executeQuery(query, pipelineProperties.get(Keys.LIMIT_VIDEO).asInstanceOf[Long])

    val vit = relatedVideo.iterator
    while(vit.hasNext) {
      breakable {
        val video: YVideo = vit.next
        if(isCached(Keys.PREFIX_VIDEO+topic, video.getId().toString())) {
          println(s"Skip: ${video.getTitle()}")
          break //go to next video
        }
        println(s"======================================\n")
        val title: String = video.getTitle().toString()
        println(s"Get comment for: $title" )

        //get the comments associated with this video
        val cList =  YoutubeDataManager
          .commentThread(video.getId().toString(),
            pipelineProperties.get(Keys.LIMIT_COMMENT).asInstanceOf[Long])

        val cit = cList.iterator
        while(cit.hasNext) {
          val comment = cit.next
          breakable {
            val authorChannelId =comment.getAuthorChannelId().toString()
            if(isCached("users", authorChannelId))
              break//continue to the next comment

            //get the user info
            val (subs, likes, channel) = processUser(authorChannelId)

            //send the subscription of this user
            subs.iterator.foreach(
              s => send(kafkaSubscriptionsProducer,Keys.PREFIX_SUBSCRIPTION+topic, s)
            )

            //send the likes of this user
            likes.iterator().foreach(
              l => send(kafkaLikeProducer, Keys.PREFIX_LIKE+topic, l)
            )

            //send the channel description
            send(kafkaChannelProducer, Keys.PREFIX_CHANNEL+topic, channel)
          }

          //sent this comment
          send(kafkaCommentProducer, Keys.PREFIX_COMMENT+topic, comment)
        }

        //get the video details 
        if(isCached(Keys.PREFIX_VIDEO+topic, video.getId().toString()))
          break

        println(s"Getting video details")
        val videoDetails : YVideo  = YoutubeDataManager.getVideo(video.getId().toString)
        send(kafkaVideoProducer,Keys.PREFIX_VIDEO+topic, videoDetails)
        cache(Keys.PREFIX_VIDEO+topic, videoDetails.getId().toString())

      } //end breakable
    } // end while
  }


  @throws(classOf[GoogleJsonResponseException])
  def processUser(userId: String) = { 
    println(s"Getting info for user: $userId\n\tSubscriptions")
    var subscriptions: Option[ju.List[YSubscription]] = None
    try {
      subscriptions = Option[ju.List[YSubscription]](YoutubeDataManager
        .getSubscriptions(userId, pipelineProperties.get(Keys.LIMIT_SUBSCRIPTION).asInstanceOf[Long]))
    } catch {
      case e: GoogleJsonResponseException => {
        //check the error message. If this exception is raised
        //because the quota limit has been exceeded then the exception is
        //passed to the caller 
        if(e.getDetails().getMessage().contains("quota"))
          throw e // throw to the caller

        //otherwise it means that subcriptions have to be taken from the list of activities of the user
        subscriptions = Option[ju.List[YSubscription]](YoutubeDataManager
          .getSubscriptionsAlternative(userId, pipelineProperties.get(Keys.LIMIT_SUBSCRIPTION).asInstanceOf[Long]))
      }
    }

    println("\tLikes")
    //get the author channel likes
    val likes: ju.List[YLike] = YoutubeDataManager
      .getLikes(userId, pipelineProperties.get(Keys.LIMIT_LIKE).asInstanceOf[Long])

    println("\tChannel")
    //get the author channel info
    val channel : YChannel = YoutubeDataManager.getChannel(userId)

    (subscriptions.getOrElse(new ju.LinkedList[YSubscription]), likes, channel)
  }


}


