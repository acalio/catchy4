package io.acalio.ytproducers.tasks
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.acalio.dm.model.avro.YChannel
import org.acalio.dm.api.YoutubeDataManager
import java.{util => ju}
import io.acalio.ytproducers.utility.Keys
import io.acalio.ytproducers.utility.KeyManager


class ChannelProducer(
  override val keyManager: KeyManager,
  override val producerProperties: ju.Properties,
  val config: ju.Properties
 ) extends KafkaTask(keyManager, producerProperties) {


  def executeImpl() {
    val topic = s"${Keys.PREFIX_CHANNEL}${config.get(Keys.TOPIC_NAME).asInstanceOf[String]}"
    val channelId = config.get(Keys.CHANNEL_ID).asInstanceOf[String]

    if(isCached(topic, channelId)) return

    val channelInfo: YChannel = YoutubeDataManager.getChannel(channelId);
    send(kafkaChannelProducer, topic, channelInfo)
    //maybe cache the new value
    cache(topic, channelId)
  }



}
