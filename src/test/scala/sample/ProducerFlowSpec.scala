package sample

import scala.concurrent.duration._

import akka.stream.scaladsl.{
  Keep,
  Sink
}
import akka.stream.testkit.scaladsl.{
  TestSource,
  TestSink
}

import org.scalatest.time.{Seconds, Span}

class ProducerFlowSpec extends AkkaStreamSpec with ProducerSpec {
  val topic = "test"

  override implicit val patienceConfig = PatienceConfig(scaled(Span(10, Seconds)))

  "producer flow" must {
    "handle records one a time" in within(10.seconds) {
      withResource(newProducer()) { producer =>
        val flowUnderTest = ProducerFlow(producer)

        val (source, sink) = TestSource.probe[ProducerFlow.InputRecord[String, String]]
          .via(flowUnderTest)
          .toMat(TestSink.probe[ProducerFlow.OutputRecord[String, String]])(Keep.both)
          .run()

        sink.request(1)
        source.sendNext(ProducerFlow.InputRecord(topic, "ciccioKey", "ciccioValue"))

        val outputRecord = sink.expectNext()
        log.debug("outputRecord {}", outputRecord)

        source.sendComplete()
        
        outputRecord.topic must be(topic)
        outputRecord.key must be("ciccioKey")
        outputRecord.value must be("ciccioValue")
      }
    }

    "handle exception on kafka send" in within(10.seconds) {
      val ex = new RuntimeException("fail!")

      withResource(newProducer(
        sendHook =
          (p, r) => {
            log.debug("handle exception sendHook")
            throw ex
          }
      )) { producer =>
        val flowUnderTest = ProducerFlow(producer)

        val (source, sink) = TestSource.probe[ProducerFlow.InputRecord[String, String]]
          .via(flowUnderTest)
          .toMat(TestSink.probe[ProducerFlow.OutputRecord[String, String]])(Keep.both)
          .run()

        sink.request(1)
        source.sendNext(ProducerFlow.InputRecord(topic, "failKey", "failValue"))
        sink.expectError(ex)
      }
    }

    "handle records in transaction" in within(10.seconds) {
      withResource(newProducer(true)) { producer =>
        producer.initTransactions

        producer.beginTransaction
        val flowUnderTest = ProducerFlow(producer)

        val (source, sink) = TestSource.probe[ProducerFlow.InputRecord[String, String]]
          .via(flowUnderTest)
          .toMat(TestSink.probe[ProducerFlow.OutputRecord[String, String]])(Keep.both)
          .run

        val count = 3
        (1 to count) foreach { i =>
          source.sendNext(ProducerFlow.InputRecord(topic, s"ciccioKey$i", s"ciccioValue$i"))
        }

        source.sendComplete()

        producer.commitTransaction

        sink.request(count)

        (1 to count) foreach { i =>
          val r = sink.requestNext()

          r.topic must be(topic)
          r.key must be(s"ciccioKey$i")
          r.value must be(s"ciccioValue$i")

          r.callbackDone.isCompleted must be(true)

          val fv = r.callbackDone.future.futureValue
          log.debug("fv {}", fv)
        }
      }
    }

    "handle error in upstream" in within(10.seconds) {
      withResource(newProducer()) { producer =>
        val flowUnderTest = ProducerFlow(producer)

        val (source, sink) = TestSource.probe[ProducerFlow.InputRecord[String, String]]
          .via(flowUnderTest)
          .toMat(TestSink.probe[ProducerFlow.OutputRecord[String, String]])(Keep.both)
          .run

        val ex = new Exception("fail!")

        sink.request(1)

        source.sendError(ex)

        sink.expectError(ex)
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
        val flowUnderTest = ProducerFlow(producer)

        val (source, sink) = TestSource.probe[ProducerFlow.InputRecord[String, String]]
          .via(flowUnderTest)
          .toMat(TestSink.probe[ProducerFlow.OutputRecord[String, String]])(Keep.both)
          .run

        sink.request(1)
        source.sendNext(ProducerFlow.InputRecord(topic, "failKey", "failValue"))
        source.sendComplete()

        val r = sink.requestNext()

        r.topic must be(topic)
        r.key must be("failKey")
        r.value must be("failValue")

        r.callbackDone.isCompleted must be(true)
        r.callbackDone.future.failed.futureValue must be(ex)
      }
    }

    "handle empty streams" in within(10.seconds) {
      withResource(newProducer()) { producer =>
        val flowUnderTest = ProducerFlow(producer)

        val (source, sink) = TestSource.probe[ProducerFlow.InputRecord[String, String]]
          .via(flowUnderTest)
          .toMat(TestSink.probe[ProducerFlow.OutputRecord[String, String]])(Keep.both)
          .run

        sink.request(1)
        source.sendComplete()
        sink.expectComplete()
      }
    }

    "simple collect" in within(10.seconds) {
      withResource(newProducer()) { producer =>
        val flowUnderTest = ProducerFlow(producer)

        val (source, future) = TestSource.probe[ProducerFlow.InputRecord[String, String]]
          .via(flowUnderTest)
          .toMat(Sink.seq)(Keep.both)
          .run

        val count = 10
        (1 to count) foreach { i =>
          source.sendNext(ProducerFlow.InputRecord(topic, s"ciccioKey$i", s"ciccioValue$i"))
        }
        source.sendComplete()

        val seq = future.futureValue

        seq.length must be(count)

        (1 to count) foreach { i =>
          val r = seq(i - 1)

          r.topic must be(topic)
          r.key must be(s"ciccioKey$i")
          r.value must be(s"ciccioValue$i")

          r.callbackDone.isCompleted must be(true)

          val fv = r.callbackDone.future.futureValue
          log.debug("fv {}", fv)
        }
      }
    }
  }
}
