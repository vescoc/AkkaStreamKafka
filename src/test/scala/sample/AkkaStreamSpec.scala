package sample

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
}
