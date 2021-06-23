package io.acalio.ytproducers.tasks
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.acalio.dm.api.YoutubeDataManager
import org.acalio.dm.model.avro.YComment
import java.util.List
import java.{util => ju}
import io.acalio.ytproducers.utility.Keys
import io.acalio.ytproducers.utility.KeyManager
import org.acalio.dm.model.avro.YChannel
import util.control.Breaks._

class CommentProducer(
  override val keyManager: KeyManager,
  override val producerProperties: ju.Properties,
  val config: ju.Properties
) extends KafkaTask(keyManager, producerProperties) {

  def executeImpl() {
    val topic = s"${Keys.PREFIX_COMMENT}${config.get(Keys.TOPIC_NAME).asInstanceOf[String]}"
    val videoId = config.get(Keys.VIDEO_ID).asInstanceOf[String]

    val commentLimit = config.get(Keys.LIMIT_COMMENT).asInstanceOf[Long]
    val cList =  YoutubeDataManager.commentThread(videoId, commentLimit)
    val it = cList.iterator
    while(it.hasNext) {
      breakable {
        val c = it.next
        if(isCached(topic, c.getId().toString()))
          break
        send(kafkaCommentProducer, topic, c)
        cache(topic, c.getId().toString())
      }
    }
  }



}

