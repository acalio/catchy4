package io.acalio.ytproducers.tasks
import io.acalio.ytproducers.utility.History
import io.acalio.ytproducers.utility.KeyManager
import io.acalio.ytproducers.utility.KeyException
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import org.acalio.dm.api.YoutubeDataManager
import org.acalio.dm.model.avro.YVideo
import org.acalio.dm.model.avro.YComment
import org.acalio.dm.model.avro.YChannel
import org.acalio.dm.model.avro.YLike
import org.acalio.dm.model.avro.YSubscription

abstract class KafkaTask(
  protected val keyManager: KeyManager,
  val producerProperties: java.util.Properties
) {

  //initialize the required producers
  val kafkaVideoProducer = new KafkaProducer[String, YVideo] (producerProperties)
  val kafkaCommentProducer = new KafkaProducer[String, YComment] (producerProperties)
  val kafkaChannelProducer = new KafkaProducer[String, YChannel] (producerProperties)
  val kafkaLikeProducer = new KafkaProducer[String, YLike] (producerProperties)
  val kafkaSubscriptionsProducer = new KafkaProducer[String, YSubscription] (producerProperties)

  var history = None : Option[History];

  @throws(classOf[GoogleJsonResponseException])
  protected def executeImpl()

  def execute() {
    try {
      executeImpl()
    } catch{
      case e: GoogleJsonResponseException => {
        //check if the quota has been exceeded
        println(e.getDetails().getMessage())
        if(e.getDetails().getMessage().contains("quota")){
          try {
            val newKey = keyManager.getKey()
            YoutubeDataManager.setDeveloperKey(newKey)
          } catch {
            case e: KeyException => {handleQuotaLimit(e); execute()}
          }
        }
      }
      case e: java.net.ConnectException => {}
    }
  }

  def setHistory(hist: History) {
    history = Some(hist)
  }

  def isCached(key: String, resource: String): Boolean = {
    history match {
      case None => return false
      case Some(value) => return value.isPresent(key, resource)
    }
  }

  def cache(key: String, resource: String) {
    history match {
      case None => {}
      case Some(value) =>  value.add(key, resource)
    }
  }


  def handleQuotaLimit(e: KeyException) {
    //there are is not any  available key
    println("All the available keys have exceeded their quota limit")
    //this thread is suspended
    val until = e.firstCheckPoint // time for the first activation
    val now = LocalDateTime.now()
    val millis = ChronoUnit.MILLIS.between(until, now)
    //put the thread to sleep
    println(s"Putting the thread to bed! See ya at...${until.toString()}")
    Thread.sleep(millis)
  }
  def send[MessageType](prod: KafkaProducer[String, MessageType], topic:String, message:MessageType) {
    prod.send(new ProducerRecord[String, MessageType](topic, message))
  }

   
}

