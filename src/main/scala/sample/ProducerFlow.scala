package sample

import scala.concurrent.Promise

import org.slf4j.LoggerFactory

import org.apache.kafka.clients.producer.{
  Producer,
  Callback,
  RecordMetadata
}

import akka.NotUsed
import akka.stream.{
  Attributes,
  Inlet,
  Outlet,
  FlowShape
}
import akka.stream.stage.{
  GraphStage,
  GraphStageLogic,
  InHandler,
  OutHandler
}
import akka.stream.scaladsl.Flow

import ProducerFlow._

class ProducerFlow[K, V](_producer: => Producer[K, V])
    extends GraphStage[FlowShape[InputRecord[K, V], OutputRecord[K, V]]] {
  val log = LoggerFactory.getLogger(getClass)

  val in = Inlet[InputRecord[K, V]]("input")
  val out = Outlet[OutputRecord[K, V]]("output")

  val shape = FlowShape.of(in, out)

  def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
    val producer = _producer

    override def preStart() {
      log.debug("preStart")
    }

    setHandler(
      in,
      new InHandler {
        def onPush() {
          log.debug("onPush")

          val inputRecord = grab(in)
          log.debug("onPush {}", inputRecord)

          try {
            val outputRecord = inputRecord.makeOutputRecord()
            producer.send(inputRecord.makeProducerRecord(), outputRecord.callback)
            push(out, outputRecord)
          } catch {
            case e: Throwable =>
              log.warn("got exception", e)
              failStage(e)
          }
        }
      }
    )

    setHandler(
      out,
      new OutHandler {
        def onPull() {
          pull(in)
        }
      }
    )
  }
}
object ProducerFlow {
  def apply[K, V](producer: => Producer[K, V]): Flow[InputRecord[K, V], OutputRecord[K, V], NotUsed] =
    Flow.fromGraph(new ProducerFlow(producer))

  trait InputRecord[K, V] extends ProducerSink.Record[K, V] {
    private[ProducerFlow] def makeOutputRecord() = DefaultOutputRecord(topic, key, value)
  }

  case class DefaultInputRecord[K, V](topic: String, key: K, value: V) extends InputRecord[K, V]
  object InputRecord {
    def apply[K, V](topic: String, key: K, value: V): InputRecord[K, V] =
      DefaultInputRecord(topic, key, value)
  }

  trait OutputRecord[K, V] extends ProducerSink.Record[K, V] {
    val callbackDone: Promise[RecordMetadata]
  }

  case class DefaultOutputRecord[K, V](
    topic: String,
    key: K,
    value: V,
    callbackDone: Promise[RecordMetadata] = Promise()
  ) extends OutputRecord[K, V] {
    lazy val callback = new Callback {
      def onCompletion(recordMetadata: RecordMetadata, exception: Exception) =
        if (exception != null)
          callbackDone.failure(exception)
        else
          callbackDone.success(recordMetadata)
    }
  }
  object OutputRecord {
    def apply[K, V](topic: String, key: K, value: V): OutputRecord[K, V] =
      DefaultOutputRecord(topic, key, value)
  }
}
