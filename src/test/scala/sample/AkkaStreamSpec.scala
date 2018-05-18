package sample

import scala.util.{
  Try,
  Failure
}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit

import org.scalatest.{MustMatchers, WordSpecLike, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures

import org.slf4j.LoggerFactory

abstract class AkkaStreamSpec
    extends TestKit(ActorSystem("test"))
    with WordSpecLike
    with MustMatchers
    with BeforeAndAfterAll
    with ScalaFutures
{
  implicit val materializer = ActorMaterializer()

  val log = LoggerFactory.getLogger(getClass)

  override def withFixture(test: NoArgTest) = {
    log.info("start <{}>", test.name)
    try
      super.withFixture(test)
    finally
      log.info("end <{}>", test.name)
  }

  def withResource[T <: AutoCloseable, V](r: => T)(f: T => V): Try[V] = {
    val resource = Try { r }
    val result = resource.map(res => f(res))
    val close = resource.flatMap(res => Try { res.close() })

    (resource, result, close) match {
      case (Failure(t), _, _) => Failure(t)
      case (_, f @ Failure(tr), Failure(tc)) =>
        tr.addSuppressed(tc)
        f
      case (_, _, Failure(tc)) => Failure(tc)
      case (_, s, _) => s
    }
  }
}
