package sample

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.{
  MockProducer,
  RecordMetadata,
  ProducerRecord,
  Callback
}

trait ProducerSpec {
  self: AkkaStreamSpec =>

  def newProducer(
    autoComplete: Boolean = true,
    sendHook: (MockProducer[String, String], java.util.concurrent.Future[RecordMetadata]) => java.util.concurrent.Future[RecordMetadata] = (_, r) => r
  ) =
    new MockProducer[String, String](autoComplete, new StringSerializer(), new StringSerializer()) {
      override def send(producerRecord: ProducerRecord[String, String], callback: Callback) = {
        val r = super.send(producerRecord, callback)
        sendHook(this, r)
      }
    }
}
