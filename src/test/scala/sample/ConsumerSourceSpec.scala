package sample

import scala.collection.JavaConverters._

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{MockConsumer, OffsetResetStrategy, ConsumerRecord}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.stream.testkit.scaladsl.TestSink

import org.scalatest.{MustMatchers, WordSpecLike, BeforeAndAfterAll}

import org.slf4j.LoggerFactory

class ConsumerSourceSpec
    extends TestKit(ActorSystem("test"))
    with WordSpecLike
    with MustMatchers
    with BeforeAndAfterAll
{
  val topic = "test"
  val partition = 0

  implicit val materializer = ActorMaterializer()

  def withResource[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
    import scala.util.control.NonFatal

    def close(e: Throwable, resource: T) {
      if (e != null) {
        try {
          resource.close
        } catch {
          case NonFatal(suppressed) =>
            e.addSuppressed(suppressed)
        } 
      } else {
        resource.close
      }
    }

    var exception: Throwable = null

    val resource = r
    try {
      f(resource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      close(exception, resource)
    }
  }

  "consumer source" must {
    "handle records one a time" in {
      withResource(new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)) { consumer =>
        val sourceUnderTest = ConsumerSource(consumer, Seq(topic))

        consumer.schedulePollTask(() => {
          val record1 = TestConsumerRecord[String, String](topic, partition, 0, null, "ciccio")
          val record2 = TestConsumerRecord[String, String](topic, partition, 1, null, "cunicio")
          val tp = new TopicPartition(topic, 0)

          consumer.rebalance(Seq(tp).asJava)
          consumer.updateBeginningOffsets(Map[TopicPartition, java.lang.Long](tp -> 0L).asJava)
          consumer.addRecord(record1)
          consumer.addRecord(record2)
        })

        sourceUnderTest
          .map(_.value)
          .runWith(TestSink.probe[String])
          .request(1)
          .expectNext("ciccio")
          .request(1)
          .expectNext("cunicio")
      }
    }

    "handle records two a time" in {
      withResource(new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)) { consumer =>
        val sourceUnderTest = ConsumerSource(consumer, Seq(topic))

        consumer.schedulePollTask(() => {
          val tp = new TopicPartition(topic, 0)
          val record1 = TestConsumerRecord[String, String](topic, partition, 0, null, "ciccio")
          val record2 = TestConsumerRecord[String, String](topic, partition, 1, null, "cunicio")

          consumer.rebalance(Seq(tp).asJava)
          consumer.updateBeginningOffsets(Map[TopicPartition, java.lang.Long](tp -> 0L).asJava)
          consumer.addRecord(record1)
          consumer.addRecord(record2)
        })

        sourceUnderTest
          .map(_.value)
          .runWith(TestSink.probe[String])
          .request(2)
          .expectNext("ciccio", "cunicio")
      }
    }

    "handle records multiple" in {
      withResource(new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)) { consumer =>
        val sourceUnderTest = ConsumerSource(consumer, Seq(topic))

        consumer.schedulePollTask(() => {
          val tp = new TopicPartition(topic, 0)
          val record = TestConsumerRecord[String, String](topic, partition, 0, null, "ciccio")

          consumer.rebalance(Seq(tp).asJava)
          consumer.updateBeginningOffsets(Map[TopicPartition, java.lang.Long](tp -> 0L).asJava)
          consumer.addRecord(record)
        })

        val t = sourceUnderTest
          .map(_.value)
          .runWith(TestSink.probe[String])
          .request(1)
          .expectNext("ciccio")

        consumer.schedulePollTask(() => {
          val record = TestConsumerRecord[String, String](topic, partition, 1, null, "cunicio")

          consumer.addRecord(record)
        })

        t
          .request(1)
          .expectNext("cunicio")
      }
    }
  }

  private object TestConsumerRecord {
    def apply[K, V](topic: String, partition: Int, offset: Long, key: K, value: V) = new ConsumerRecord(topic, partition, offset, key, value)
  }
}
