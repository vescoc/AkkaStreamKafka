package sample

import scala.concurrent.duration._

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.MockProducer

import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource

import org.scalatest.time.{Seconds, Span}

class ProducerSinkSpec extends AkkaStreamSpec {
  val topic = "test"

  override implicit val patienceConfig = PatienceConfig(scaled(Span(10, Seconds)))

  "producer sink" must {
    "handle records one a time" in within(10.seconds) {
      withResource(new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())) { producer =>
        val sinkUnderTest = ProducerSink(producer)

        val (source, future) = TestSource.probe[ProducerSink.Record[String, String]]
          .toMat(sinkUnderTest)(Keep.both)
          .run

        source.sendNext(ProducerSink.Record(topic, "ciccioKey", "ciccioValue"))
        source.sendComplete()

        future.futureValue
      }
    }

    "handle records in transaction" in within(10.seconds) {
      withResource(new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())) { producer =>
        producer.initTransactions

        producer.beginTransaction
        val sinkUnderTest = ProducerSink(producer)

        val (source, future) = TestSource.probe[ProducerSink.Record[String, String]]
          .toMat(sinkUnderTest)(Keep.both)
          .run

        source.sendNext(ProducerSink.Record(topic, "ciccioKey1", "ciccioValue1"))
        source.sendNext(ProducerSink.Record(topic, "ciccioKey2", "ciccioValue2"))
        source.sendNext(ProducerSink.Record(topic, "ciccioKey3", "ciccioValue3"))
        source.sendComplete()

        producer.commitTransaction

        future.futureValue
      }
    }

    "handle error" in within(10.seconds) {
      withResource(new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())) { producer =>
        val sinkUnderTest = ProducerSink(producer)

        val (source, future) = TestSource.probe[ProducerSink.Record[String, String]]
          .toMat(sinkUnderTest)(Keep.both)
          .run

        val ex = new Exception("fail!")
        
        source.sendError(ex)

        future.failed.futureValue must be(ex)
      }
    }
  }
}
