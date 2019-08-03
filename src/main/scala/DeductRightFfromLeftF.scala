import java.io.{File, PrintWriter}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

object DeductRightFfromLeftF extends App {

  def isInRight(str: String, rightX: List[String]): Boolean = rightX match {
    case x :: xs =>
      if (x == str) true
      else isInRight(str, xs)
    case Nil => false
  }

  val start = System.currentTimeMillis()

  // Not in memory, iterator
  val leftIter = Source.fromFile("/home/heroys6/archieve/DEP-75+/old_new_PP2_PP1_ext/new_trashless_uniq_united_PP2_PP1_extended.csv", "UTF-8")
  val left = leftIter.getLines()

  // In memory. Usually it's pretty small
  val rightIter = Source.fromFile("/home/heroys6/archieve/DEP-75+/old_new_PP2_PP1_ext/old_trashless_qniq_united_PP2_PP-ext.txt", "UTF-8")
  val right = rightIter
    .getLines()
    .toList
    .groupBy(identity)

  val pw = new PrintWriter("left_without_right_.csv", "UTF-8")

  import akka.stream.scaladsl.Source

  implicit val sys = ActorSystem("actor-system")
  implicit val materializer = ActorMaterializer()

  Source.fromIterator(() => left)
    .mapAsyncUnordered(32) { str =>
      Future {
        (str, right.contains(str))
      }
    }
    .filterNot(_._2)
    .mapAsyncUnordered(32) { x =>
      Future(pw.println(x._1))
    }
    .runWith(Sink.ignore)
    .andThen {
      case _try => {
        pw.close()
        rightIter.close()
        leftIter.close()
        _try match {
          case Success(s) =>
            println(s"Done successfully. Time: ${(System.currentTimeMillis().toDouble - start) / 1000}")
          case Failure(f) =>
            println(s"Failed with error: $f")
        }
      }
    }
}