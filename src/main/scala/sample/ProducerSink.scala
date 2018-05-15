package sample

import scala.concurrent.{Future, Promise}

import org.slf4j.LoggerFactory

import akka.Done
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.scaladsl.Sink
import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, InHandler}

import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}

import ProducerSink._

class ProducerSink[K, V](_producer: => Producer[K, V])
    extends GraphStageWithMaterializedValue[SinkShape[ProducerSink.Record[K, V]], Future[Done]] {
  val log = LoggerFactory.getLogger(getClass)

  val in = Inlet[Record[K, V]]("producerRecords")

  override val shape = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val producer = _producer

    var callback: AsyncCallback[(RecordMetadata, Exception)] = null

    var count = 0L

    val graphStage = new GraphStageLogic(shape) {
      def gotAck(info: (RecordMetadata, Exception)) = {
        count = count - 1

        log.debug("gotAck {} {}", info, count)
        val (_, exception) = info

        if (exception != null) {
          promise.tryFailure(exception)

          if (!isClosed(in))
            failStage(exception)
        } else {
          if (count == 0 && isClosed(in))
            promise.trySuccess(Done)
        }
      }

      val cb = new Callback {
        def onCompletion(recordMetadata: RecordMetadata, exception: Exception) {
          log.debug("onCompletion {}", (recordMetadata, exception))
          callback.invoke((recordMetadata, exception))
        }
      }

      override def preStart = {
        log.debug("preStart")
        callback = getAsyncCallback(gotAck)
        pull(in)
      }

      setHandler(
        in,
        new InHandler {
          def onPush = {
            log.debug("onPush")

            val record = grab(in)

            log.debug("onPush {} {}", record, count)

            try {
              producer.send(record.makeProducerRecord, cb)

              count = count + 1

              pull(in)
            } catch {
              case e: Throwable =>
                log.warn("got exception {}", e)
                failStage(e)
            }
          }

          override def onUpstreamFinish = {
            log.debug("onUpstreamFinish {} {}", promise, count)
            if (count == 0)
              promise.trySuccess(Done)

            super.onUpstreamFinish
          }

          override def onUpstreamFailure(ex: Throwable) = {
            log.debug("onUpstreamFailure {}", ex)
            if (count == 0)
              promise.tryFailure(ex)

            log.debug("onUpstreamFailure promise {}", promise)

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

  case class Record[K, V](topic: String, key: K, value: V) {
    private[ProducerSink] def makeProducerRecord = new ProducerRecord(topic, key, value)
  }
}
