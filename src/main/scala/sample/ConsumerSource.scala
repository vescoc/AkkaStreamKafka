package sample

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}

import akka.NotUsed
import akka.stream.{SourceShape, Outlet, Attributes}
import akka.stream.stage.{GraphStage, TimerGraphStageLogic, OutHandler}
import akka.stream.scaladsl.Source

import ConsumerSource._

class ConsumerSource[K, V](_consumer: => Consumer[K, V], topics: Seq[String], timeout: FiniteDuration, sleepTime: FiniteDuration) extends GraphStage[SourceShape[Record[K, V]]] {
  val log = LoggerFactory.getLogger(getClass)

  val out = Outlet[Record[K, V]]("consumerRecords")

  override val shape = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes) = new TimerGraphStageLogic(shape) {
    val consumer = _consumer

    consumer.subscribe(topics.asJavaCollection)

    override protected def onTimer(timerKey: Any) {
      log.debug("onTimer {}", timerKey)
      doPoll()
    }

    private def nextHandler(iterator: Iterator[ConsumerRecord[K, V]]) = new OutHandler {
      def doPush =
        if (iterator.hasNext) {
          val consumerRecord = iterator.next

          val record = Record(consumerRecord)

          log.debug("nextHandler push {}", record)
          push(out, record)
        } else {
          log.debug("nextHandler schedule")
          scheduleOnce(None, sleepTime)
        }

      doPush

      def onPull {
        log.debug("nextHandler onPull")
        doPush
      }
    }

    private def doPoll() = {
      log.debug("doPoll")
      val consumerRecords = consumer.poll(timeout.toMillis)
      log.debug("doPoll consumerRecords count {}", consumerRecords.count)

      val iterator = consumerRecords.asScala.iterator
      setHandler(out, nextHandler(iterator))
    }

    setHandler(out, new OutHandler {
      def onPull {
        log.debug("initHandler onPull")
        doPoll()
      }
    })
  }
}
object ConsumerSource {
  def apply[K, V](consumer: => Consumer[K, V], topics: Seq[String], timeout: FiniteDuration = 100.millis, sleepTime: FiniteDuration = 1.second): Source[Record[K, V], NotUsed] = Source.fromGraph(new ConsumerSource(consumer, topics, timeout, sleepTime))

  case class Record[K, V](consumerRecord: ConsumerRecord[K, V]) {
    def value = consumerRecord.value
  }
}
