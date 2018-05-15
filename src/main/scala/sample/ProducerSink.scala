package sample

import scala.concurrent.Future

import org.slf4j.LoggerFactory

import akka.NotUsed
import akka.stream.{SinkShape, Inlet, Attributes}
import akka.stream.scaladsl.Sink
import akka.stream.stage.{
  GraphStageWithMaterializedValue,
  InHandler,
  GraphStageLogic
}

import org.apache.kafka.clients.producer.Producer

import ProducerSink._

class ProducerSink[K, V](_producer: => Producer[K, V])
    extends GraphStageWithMaterializedValue[SinkShape[
                                              ProducerSink.Record[K, V]],
                                            Future[Nothing]] {
  val log = LoggerFactory.getLogger(getClass)

  val in = Inlet[Record[K, V]]("producerRecords")

  override val shape = SinkShape(in)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic, Future[Nothing]) = ???
}
object ProducerSink {
  def apply[K, V](
      producer: => Producer[K, V]): Sink[Record[K, V], Future[Nothing]] =
    Sink.fromGraph(new ProducerSink(producer))

  case class Record[K, V](topic: String, key: K, value: V)
}
