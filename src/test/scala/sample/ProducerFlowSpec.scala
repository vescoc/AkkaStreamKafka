package sample

import scala.concurrent.duration._

import akka.stream.scaladsl.Keep
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

        sink.request(3)
        source.sendNext(ProducerFlow.InputRecord(topic, "ciccioKey", "ciccioValue"))

        val outputRecord = sink.expectNext()
        log.debug("outputRecord {}", outputRecord)

        outputRecord.topic must be(topic)
        outputRecord.key must be("ciccioKey")
        outputRecord.value must be("ciccioValue")
      }
    }
  }
}
