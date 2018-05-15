package sample

import scala.collection.JavaConverters._

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{MockConsumer, OffsetResetStrategy, ConsumerRecord}

import akka.stream.testkit.scaladsl.TestSink

class ConsumerSourceSpec extends AkkaStreamSpec {
  val topic = "test"
  val partition = 0

  "consumer source" must {
    "handle records one a time" in {
      withResource(new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)) { consumer =>
        val tp = new TopicPartition(topic, partition)

        val sourceUnderTest = ConsumerSource(consumer, Seq(topic))

        consumer.schedulePollTask(() => {
          val record1 = TestConsumerRecord[String, String](topic, partition, 0, null, "ciccio")
          val record2 = TestConsumerRecord[String, String](topic, partition, 1, null, "cunicio")

          consumer.rebalance(Seq(tp).asJava)
          consumer.updateBeginningOffsets(Map[TopicPartition, java.lang.Long](tp -> 0L).asJava)
          consumer.addRecord(record1)
          consumer.addRecord(record2)
        })

        sourceUnderTest
          .map { x => (x.key, x.value) }
          .runWith(TestSink.probe[(String, String)])
          .request(1)
          .expectNext((null, "ciccio"))
          .request(1)
          .expectNext((null, "cunicio"))
      }
    }

    "handle records two a time" in {
      withResource(new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)) { consumer =>
        val tp = new TopicPartition(topic, partition)

        val sourceUnderTest = ConsumerSource(consumer, Seq(topic))

        consumer.schedulePollTask(() => {
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
        val tp = new TopicPartition(topic, partition)

        val sourceUnderTest = ConsumerSource(consumer, Seq(topic))

        consumer.schedulePollTask(() => {
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

    "sync commit a record" in {
      withResource(new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)) { consumer =>
        val tp = new TopicPartition(topic, partition)

        val sourceUnderTest = ConsumerSource(consumer, Seq(topic))

        consumer.schedulePollTask(() => {
          val record1 = TestConsumerRecord[String, String](topic, partition, 0, null, "ciccio")

          consumer.rebalance(Seq(tp).asJava)
          consumer.updateBeginningOffsets(Map[TopicPartition, java.lang.Long](tp -> 0L).asJava)
          consumer.addRecord(record1)
        })

        sourceUnderTest
          .map { r =>
            r.commitSync
            r
          }
          .map(_.value)
          .runWith(TestSink.probe[String])
          .request(1)
          .expectNext("ciccio")

        consumer.committed(tp).offset must be(0)
      }
    }

    "sync commit two records, bad order" in {
      withResource(new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)) { consumer =>
        val tp = new TopicPartition(topic, partition)

        val sourceUnderTest = ConsumerSource(consumer, Seq(topic))

        consumer.schedulePollTask(() => {
          val record1 = TestConsumerRecord[String, String](topic, partition, 0, null, "ciccio")
          val record2 = TestConsumerRecord[String, String](topic, partition, 1, null, "cunicio")

          consumer.rebalance(Seq(tp).asJava)
          consumer.updateBeginningOffsets(Map[TopicPartition, java.lang.Long](tp -> 0L).asJava)
          consumer.addRecord(record1)
          consumer.addRecord(record2)
        })

        sourceUnderTest
          .grouped(2)
          .map { l =>
            println(s"l=$l")
            l(1).commitSync
            l(0).commitSync // nop
            l
          }
          .runWith(TestSink.probe[Seq[ConsumerSource.Record[String, String]]])
          .request(1)
          .expectNext

        val c = consumer.committed(tp)
        c.offset must be(1)
      }
    }
  }

  private object TestConsumerRecord {
    def apply[K, V](topic: String, partition: Int, offset: Long, key: K, value: V) = new ConsumerRecord(topic, partition, offset, key, value)
  }
}
