package io.acalio.ytproducers.tasks
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.acalio.dm.api.YoutubeDataManager
import org.acalio.dm.model.avro.YLike
import java.util.List
import java.{util => ju}
import io.acalio.ytproducers.utility.Keys
import io.acalio.ytproducers.utility.KeyManager

class LikeProducer (
  override val keyManager: KeyManager,
  override val producerProperties: ju.Properties,
  val config: ju.Properties
) extends KafkaTask(keyManager, producerProperties) {

  def executeImpl() {

    val topic = s"${Keys.PREFIX_LIKE}${config.get(Keys.TOPIC_NAME).asInstanceOf[String]}"

    val channelId: String = config.get(Keys.CHANNEL_ID).asInstanceOf[String]
      val maxResult: Long = config.get(Keys.LIMIT_LIKE).asInstanceOf[Long]

    println(s"getting likes for channel:${channelId}")
    val likes : List[YLike] = YoutubeDataManager.getLikes(channelId, maxResult)

    val it = likes.iterator
    while(it.hasNext)
      send(kafkaLikeProducer, topic, it.next)
  }
}
