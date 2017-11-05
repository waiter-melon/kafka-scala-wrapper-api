import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}
import cakesolutions.kafka.KafkaProducer.Conf
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.{Failure, Success}

/**
  * Kafka Producer
  */
object Producer extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  val conf = ConfigFactory.load("application.conf") // loads from configuration.conf

  val producer = KafkaProducer[String, String](Conf(conf, new StringSerializer(), new StringSerializer()))

  val TOPIC = "first_topic"

  for (i <- 1 to 10) {
    val record = KafkaProducerRecord(TOPIC, Some(Integer.toString(i)), s"hello $i")
    producer.send(record).
      onComplete {
        case Success(r) => println("offset: " + r.offset + " partition: " + r.partition)
        case Failure(e) => e.printStackTrace()
      }
  }

  producer.close()

}