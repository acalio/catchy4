package io.acalio.ytproducers.tasks
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.acalio.dm.api.YoutubeDataManager
import org.acalio.dm.model.avro.YVideo
import java.util.List
import java.{util => ju}
import io.acalio.ytproducers.utility.Keys
import io.acalio.ytproducers.utility.KeyManager

class VideoProducer(
  override val keyManager: KeyManager,
  override val producerProperties: ju.Properties,
  val config: ju.Properties
) extends KafkaTask(keyManager, producerProperties) {

  def executeImpl() {
    val topic = s"${Keys.PREFIX_VIDEO}-${config.get(Keys.TOPIC_NAME).asInstanceOf[String]}"

    val videoId = config.get(Keys.VIDEO_ID).asInstanceOf[String]
    val video =  YoutubeDataManager.getVideo(videoId)
    send(kafkaVideoProducer, topic, video)
  }

}

