import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import slick.jdbc.PostgresProfile.api._

import scala.util.{Failure, Success, Try}

class PhotStock(tag: Tag,
                override val schemaName: Option[String] = None,
                override val tableName: String = "def")
  extends Table[(Option[Int], String, Double)](tag, schemaName, tableName) {
  def id = column[Option[Int]]("id", O.PrimaryKey, O.AutoInc)

  def query = column[String]("query")

  def percents = column[Double]("percents")

  def * = (id, query, percents)
}

object PhotStock {
  def forSchema(schemaName: String = "deposit", tableName: String): TableQuery[PhotStock] =
    new TableQuery(new PhotStock(_, Some(schemaName), tableName))
}

class RelevantUrlTop100(tag: Tag,
                override val schemaName: Option[String] = None,
                override val tableName: String = "def")
  extends Table[(Option[Int], String, String, Int)](tag, schemaName, tableName) {
  def id = column[Option[Int]]("id", O.PrimaryKey, O.AutoInc)

  def query = column[String]("query")

  def url = column[String]("url")

  def position = column[Int]("position")

  def * = (id, query, url, position)
}

object RelevantUrlTop100 {
  def forSchema(schemaName: String = "deposit", tableName: String): TableQuery[RelevantUrlTop100] =
    new TableQuery(new RelevantUrlTop100(_, Some(schemaName), tableName))
}

object UploadPhotStockRelevant /*extends App*/ {
  val start = System.currentTimeMillis()

  implicit private val exc = ExecutionContext.Implicits.global
  implicit private val actorSystem = ActorSystem()
  implicit private val mat = ActorMaterializer()

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val db = Database.forConfig("db")

  /*val iterPhoto = Source.fromFile("/home/heroys6/Documents/result_deposit_percents.tsv").getLines()
  iterPhoto.next() // -- header
  val iterStock = Source.fromFile("/home/heroys6/Documents/result_deposit_stock1.tsv").getLines()
  iterStock.next() // -- header*/
  val iterRelevantUrlTop100 = Source.fromFile("/home/heroys6/scripts/deposit/decode_url/decoded_result_deposit_top100.tsv").getLines()
  iterRelevantUrlTop100.next() // -- header

  import akka.stream.scaladsl.Source

  /*for {
    _ <- db.run(PhotStock.forSchema(tableName = "photo_percents").schema.create)
    _ <- Source.fromIterator(() => iterPhoto)
      .mapAsyncUnordered(32) { row =>
        val splitted = row.split("\t")
        if (Try(splitted(0)).isSuccess && Try(splitted(1)).isSuccess) {
          Future(Some(None, splitted(0), splitted(1).toDouble))
        }
        else {
          println("Smth wrong with line: " + row)
          Future.successful(None)
        }
      }
      .filter(_.isDefined)
      .grouped(500)
      .mapAsync(4)(x => db.run(PhotStock.forSchema(tableName = "photo_percents") ++= x.flatten))
      .runWith(Sink.ignore)
      .andThen {
        case Success(s) =>
          logger.info(s"""Success. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
        case Failure(f) =>
          logger.info(s"""Failure. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
          f.printStackTrace()
      }
  } yield Done*/

  /*for {
    _ <- db.run(PhotStock.forSchema(tableName = "stock_percents").schema.create)
    _ <- Source.fromIterator(() => iterStock)
      .mapAsyncUnordered(32) { row =>
        val splitted = row.split("\t")
        if (Try(splitted(0)).isSuccess && Try(splitted(1)).isSuccess) {
          Future(Some(None, splitted(0), splitted(1).toDouble))
        }
        else {
          println("Smth wrong with line: " + row)
          Future.successful(None)
        }
      }
      .filter(_.isDefined)
      .grouped(500)
      .mapAsync(8)(x => db.run(PhotStock.forSchema(tableName = "stock_percents") ++= x.flatten))
      .runWith(Sink.ignore)
      .andThen {
        case Success(s) =>
          logger.info(s"""Success. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
        case Failure(f) =>
          logger.info(s"""Failure. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
          f.printStackTrace()
      }
  } yield Done*/

  for {
    _ <- db.run(RelevantUrlTop100.forSchema(tableName = "relevant_url_top100").schema.create)
    _ <- Source.fromIterator(() => iterRelevantUrlTop100)
      .mapAsyncUnordered(32) { row =>
        val splitted = row.split("\t")
        if (Try(splitted(0)).isSuccess && Try(splitted(1)).isSuccess && Try(splitted(2)).isSuccess) {
          Future(Some(None, splitted(0), splitted(1), splitted(2).toInt))
        }
        else {
          println("Smth wrong with line: " + row)
          Future.successful(None)
        }
      }
      .filter(_.isDefined)
      .grouped(500)
      .mapAsync(8)(x => db.run(RelevantUrlTop100.forSchema(tableName = "relevant_url_top100") ++= x.flatten))
      .runWith(Sink.ignore)
      .andThen {
        case Success(s) =>
          logger.info(s"""Success. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
        case Failure(f) =>
          logger.info(s"""Failure. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
          f.printStackTrace()
      }
  } yield Done
}
