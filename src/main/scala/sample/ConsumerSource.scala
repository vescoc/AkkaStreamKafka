package sample

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, OffsetAndMetadata}

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

    private var committedTopicPartitions = Map.empty[(String, Int), Long]

    private def getCommit(consumerRecord: ConsumerRecord[K, V]): Option[CommitInfo] = {
      def make = {
        committedTopicPartitions = committedTopicPartitions + ((consumerRecord.topic, consumerRecord.partition) -> consumerRecord.offset)

        Some(CommitInfo(
          new TopicPartition(consumerRecord.topic, consumerRecord.partition),
          new OffsetAndMetadata(consumerRecord.offset)
        ))
      }

      committedTopicPartitions.get((consumerRecord.topic, consumerRecord.partition)) match {
        case Some(l) if l < consumerRecord.offset =>
          make
        case Some(_) =>
          None
        case None =>
          make
      }
    }

    private def nextHandler(iterator: Iterator[ConsumerRecord[K, V]]) = new OutHandler {
      def doPush =
        if (iterator.hasNext) {
          val _consumerRecord = iterator.next

          val record = new Record[K, V] {
            val consumerRecord = _consumerRecord

            def commitSync = getCommit(consumerRecord) foreach { x => consumer.commitSync(Map(x.topic -> x.offset).asJava) }
          }

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

    override protected def onTimer(timerKey: Any) {
      log.debug("onTimer {}", timerKey)
      doPoll()
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

  trait Record[K, V] {
    val consumerRecord: ConsumerRecord[K, V]

    def key = consumerRecord.key
    def value = consumerRecord.value

    def commitSync: Unit

    override def toString = s"Record[${consumerRecord}]"
  }

  private [ConsumerSource] case class CommitInfo(topic: TopicPartition, offset: OffsetAndMetadata)
}
