import java.util

import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.KafkaConsumer.Conf
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetResetStrategy}
import org.apache.kafka.common.serialization.StringDeserializer

object Consumer extends App {

  val conf = Conf(new StringDeserializer(), new StringDeserializer(),
    bootstrapServers = "localhost:9092",
    groupId = "something",
    enableAutoCommit = true,
    autoOffsetReset = OffsetResetStrategy.LATEST,
    autoCommitInterval = 1000)

  val consumer = KafkaConsumer[String, String](conf)

  val TOPIC = "first_topic"

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while (true) {
    val records: ConsumerRecords[String, String] = consumer.poll(100) // poll for 100 seconds
    records.forEach(r => println("val: " + r.value + " key: " + r.key + " offset: " + r.offset + " partition: " + r.partition))
  }

}
