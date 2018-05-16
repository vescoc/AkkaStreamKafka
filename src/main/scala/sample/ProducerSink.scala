package sample

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.{
  Future,
  Promise
}

import org.slf4j.LoggerFactory

import akka.Done
import akka.stream.{
  Attributes,
  Inlet,
  SinkShape
}
import akka.stream.scaladsl.Sink
import akka.stream.stage.{
  AsyncCallback,
  GraphStageLogic,
  GraphStageWithMaterializedValue,
  InHandler
}

import org.apache.kafka.clients.producer.{
  Callback,
  Producer,
  ProducerRecord,
  RecordMetadata
}

import ProducerSink._

class ProducerSink[K, V](_producer: => Producer[K, V])
    extends GraphStageWithMaterializedValue[SinkShape[ProducerSink.Record[K, V]], Future[Done]] {
  val log = LoggerFactory.getLogger(getClass)

  val in = Inlet[Record[K, V]]("producerRecords")

  override val shape = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val producer = _producer

    val graphStage = new GraphStageLogic(shape) {
      val count = new AtomicLong(0)

      var callback: AsyncCallback[Exception] = null

      def countIsZero() = count.compareAndSet(0, 0)

      val cb = new Callback {
        def onCompletion(recordMetadata: RecordMetadata, exception: Exception) {
          log.debug("onCompletion {}", Array(recordMetadata, exception): _*)

          count.decrementAndGet

          if (exception != null) {
            promise.tryFailure(exception)
            callback.invoke(exception)
          } else if (countIsZero() && isClosed(in)) // TODO: check sync
            promise.trySuccess(Done)
        }
      }

      override def preStart() {
        log.debug("preStart")

        callback = getAsyncCallback(x => { failStage(x) })

        // the show must go on...
        pull(in)
      }

      setHandler(
        in,
        new InHandler {
          def onPush() {
            log.debug("onPush")

            val record = grab(in)
            log.debug("onPush {} {}", Array(record, count): _*)

            try {
              producer.send(record.makeProducerRecord(), cb)
              count.incrementAndGet
              pull(in)
            } catch {
              case e: Throwable =>
                log.warn("onPush got exception on send", e)
                promise.tryFailure(e)
                failStage(e)
            }
          }

          override def onUpstreamFinish() {
            log.debug("onUpstreamFinish {} {}", Array(promise, count): _*)
            if (countIsZero())
              promise.trySuccess(Done)

            super.onUpstreamFinish
          }

          override def onUpstreamFailure(ex: Throwable) {
            log.debug("onUpstreamFailure", ex)
            promise.tryFailure(ex)

            super.onUpstreamFailure(ex)
          }
        }
      )
    }

    (graphStage, promise.future)
  }
}
object ProducerSink {
  def apply[K, V](producer: => Producer[K, V]): Sink[Record[K, V], Future[Done]] =
    Sink.fromGraph(new ProducerSink(producer))

  trait Record[K, V] {
    val topic: String
    val key: K
    val value: V

    private[sample] def makeProducerRecord() = new ProducerRecord(topic, key, value)
  }

  case class DefaultRecord[K, V](topic: String, key: K, value: V) extends Record[K, V]
  object Record {
    def apply[K, V](topic: String, key: K, value: V) = new DefaultRecord(topic, key, value)
  }
}
