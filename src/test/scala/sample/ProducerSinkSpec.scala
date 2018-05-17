package sample

import scala.concurrent.duration._

import akka.Done
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource

import org.scalatest.time.{Seconds, Span}

class ProducerSinkSpec extends AkkaStreamSpec with ProducerSpec {
  val topic = "test"

  override implicit val patienceConfig = PatienceConfig(scaled(Span(10, Seconds)))

  "producer sink" must {
    "handle records one a time" in within(10.seconds) {
      withResource(newProducer()) { producer =>
        val sinkUnderTest = ProducerSink(producer)

        val (source, future) = TestSource.probe[ProducerSink.Record[String, String]]
          .toMat(sinkUnderTest)(Keep.both)
          .run

        source.sendNext(ProducerSink.Record(topic, "ciccioKey", "ciccioValue"))
        source.sendComplete()

        future.futureValue must be(Done)
      }
    }

    "handle records in transaction" in within(10.seconds) {
      withResource(newProducer()) { producer =>
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

        future.futureValue must be(Done)
      }
    }

    "handle error in upstream" in within(10.seconds) {
      withResource(newProducer()) { producer =>
        val sinkUnderTest = ProducerSink(producer)

        val (source, future) = TestSource.probe[ProducerSink.Record[String, String]]
          .toMat(sinkUnderTest)(Keep.both)
          .run

        val ex = new Exception("fail!")
        
        source.sendError(ex)

        future.failed.futureValue must be(ex)
      }
    }

    "handle error in kafka callback" in within(10.seconds) {
      val ex = new RuntimeException("fail!")
      withResource(newProducer(false,
        (p, r) => {
          val v = p.errorNext(ex)
          log.debug("sendHook {}", v)
          r
        })) { producer =>
        val sinkUnderTest = ProducerSink(producer)

        val (source, future) = TestSource.probe[ProducerSink.Record[String, String]]
          .toMat(sinkUnderTest)(Keep.both)
          .run

        source.sendNext(ProducerSink.Record(topic, "failKey", "failValue"))
        source.sendComplete()

        future.failed.futureValue must be(ex)
      }
    }

    "handle exception on kafka send" in within(10.seconds) {
      val ex = new RuntimeException("fail!")
      withResource(newProducer(false,
        (p, r) => {
          throw ex
        })) { producer =>
        val sinkUnderTest = ProducerSink(producer)

        val (source, future) = TestSource.probe[ProducerSink.Record[String, String]]
          .toMat(sinkUnderTest)(Keep.both)
          .run

        source.sendNext(ProducerSink.Record(topic, "failKey", "failValue"))
        source.sendComplete()

        future.failed.futureValue must be(ex)
      }
    }

    "handle later ack" in within(10.seconds) {
      withResource(newProducer(false)) { producer =>
        val sinkUnderTest = ProducerSink(producer)

        val (source, future) = TestSource.probe[ProducerSink.Record[String, String]]
          .toMat(sinkUnderTest)(Keep.both)
          .run

        source.sendNext(ProducerSink.Record(topic, "okKey", "okValue"))
        source.sendComplete()

        Thread.sleep(1000) // bad bad bad!!!
        val v = producer.completeNext()
        log.debug("handle later ack {}", v)

        producer.flush()

        future.futureValue must be(Done)
      }
    }

    "handle empty streams" in within(10.seconds) {
      withResource(newProducer()) { producer =>
        val sinkUnderTest = ProducerSink(producer)

        val (source, future) = TestSource.probe[ProducerSink.Record[String, String]]
          .toMat(sinkUnderTest)(Keep.both)
          .run
        source.sendComplete()

        future.futureValue must be(Done)
      }
    }
  }
}
