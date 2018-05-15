package sample

import org.apache.kafka.clients.producer.MockProducer

import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource

class ProducerSinkSpec extends AkkaStreamSpec {
  "producer sink" must {
    "handle records one a time" in {
      withResource(new MockProducer[String, String]()) { producer =>
        val sinkUnderTest = ProducerSink(producer)

        val source = TestSource.probe[ProducerSink.Record[String, String]]
          .toMat(sinkUnderTest)(Keep.left)
      }
    }
  }
}
