import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source => AkkaSource}
import akka.stream.{ActorMaterializer, Materializer}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import slick.jdbc.PostgresProfile.api._

import scala.util.{Failure, Success, Try}

object uploadDepositFull extends App {

  case class LiftedFullStats(id: Rep[Option[Int]], query: Rep[String],
                             phot: Rep[Double], stock: Rep[Double],
                             picBlock: Rep[Int], topPosition: Rep[Int],
                             firstRelevant: Rep[String], secondRelevant: Rep[String])

  case class FullStats(id: Option[Int], query: String, phot: Double,
                       stock: Double, picBlock: Int, topPosition: Int,
                       firstRelevant: String, secondRelevant: String)

  implicit object FullStatsShape extends CaseClassShape(LiftedFullStats.tupled, FullStats.tupled)

  class DBFullStats(tag: Tag, override val schemaName: Option[String] = None)
    extends Table[FullStats](tag, schemaName, "full_stat") {

    def id = column[Option[Int]]("id", O.AutoInc, O.PrimaryKey)
    def query = column[String]("query")
    def phot = column[Double]("phot")
    def stock = column[Double]("stock")
    def picBlock = column[Int]("pic_block")
    def topPosition = column[Int]("top_position")
    def firstRelevant = column[String]("first_relevant")
    def secondRelevant = column[String]("second_relevant")

    def * = LiftedFullStats(id, query, phot, stock, picBlock, topPosition, firstRelevant, secondRelevant)
  }

  val start = System.currentTimeMillis()

  implicit private val exc = ExecutionContext.Implicits.global
  implicit private val actorSystem = ActorSystem()
  implicit private val mat = ActorMaterializer()

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val db = Database.forConfig("db")

  // Structure: query	phot	stock	pic_block	top_position	first_relevant	second_relevant
  val file = Source.fromFile("/home/heroys6/PycharmProjects/pic_block_fix/res_pic_block_fix.csv").getLines()
  file.next() // remove header
  val dbFullStats = new TableQuery(new DBFullStats(_, Some("deposit")))

  for {
    _ <- db.run(dbFullStats.schema.create)
      //.recover{ case _ => Future.successful() }
      .andThen {
      case Success(_) =>
        logger.info(s"""Table has been created. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
      case Failure(f) =>
        logger.info(s"""Failure. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
        f.printStackTrace()
    }
    _ <- AkkaSource.fromIterator(() => file)
      .mapAsyncUnordered(32) { row => Future {
        val spl = row.split("\t")

        if (spl.length != 7) None
        else
          Some(FullStats(None, spl(0), spl(1).toDouble, spl(2).toDouble,
            spl(3).toInt, if (spl(4) == "-") 0 else spl(4).toInt, spl(5), spl(6)))
      }}
      .filter(_.isDefined)
      .map(_.get)
      .grouped(500)
      // 24K/sec 1.5 mln
      /*.mapAsyncUnordered(8) { bulk => Future {
        dbFullStats ++= bulk
      }}
      .runForeach(bulkInsert => db.run(bulkInsert))*/
      // 14K/sec 4.1 mln
      .mapAsyncUnordered(8)(x => db.run(dbFullStats ++= x))
      .runWith(Sink.ignore)
      .andThen {
        case Success(_) =>
          logger.info(s"""Success. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
        case Failure(f) =>
          logger.info(s"""Failure. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
          f.printStackTrace()
      }
  } yield Done
}
