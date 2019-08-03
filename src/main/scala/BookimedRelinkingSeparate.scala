import java.io.{File, PrintWriter}

import BookimedRelinking.pwClinic
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

object BookimedRelinkingSeparate extends App {

  //q f cu exep domain count pos
  val start = System.currentTimeMillis()

  case class BookData(cluster_id: String, query: String, frequency: Int, fin_cluster_url: String,
                      excep: String = "", domain: String = "https://ru.bookimed.com",
                      count: Int = 0, google_position: Int)

  case class ArticleClinic(query: String, frequency: Int, fin_cluster_url: String, quantity: Int)

  def findQuantity(all: Int, rows: List[ArticleClinic]): List[ArticleClinic] = {
    val common = all / rows.length
    val reserv = all - (common * rows.length)

    //println(s"""all: $all, rows.length: ${rows.length}, common: $common, reserv: $reserv""")

    val updated = rows.map(_.copy(quantity = common))
    val luckies = for (i <- 1 to reserv)
      yield updated(i - 1).copy(quantity = updated(i - 1).quantity + 1)

    luckies.toList ::: updated.diff(updated.take(reserv))
  }

  val bookData = Source.fromFile("/home/heroys6/archieve/BK-620/book_data.csv", "UTF-8")
    .getLines().toList.tail
    .flatMap { row =>
      val splitted = row.split('\t')

      splitted.length match {
        case 18 =>
          splitted(12) match {
            case "-" => None
            case fin_cluster_url =>

              val cust_freq = splitted(10) match {
                case "-" => 0
                case x => x.toInt
              }
              val cust_g_pos = splitted(8) match {
                case "-" => 0
                case x => x.toInt
              }

              Some(
                BookData(cluster_id = splitted(0),
                  query = splitted(2).toLowerCase,
                  frequency = cust_freq,
                  fin_cluster_url = fin_cluster_url,
                  google_position = cust_g_pos)
              )
          }
        case _ =>
          println(s"""Skip [book data] row: "$row"""")
          None
      }
    }
    .filter(_.google_position > 3)

  val pwArticle = new PrintWriter("/home/heroys6/archieve/BK-620/separate/outp-article.csv", "UTF-8")

  pwArticle.println("query\tfrequency\tfin_clust_url\texception\tdomain\tlinks_per_q\tposition")

  val article_iter = Source.fromFile("/home/heroys6/archieve/BK-620/article.tsv", "UTF-8")
  val article_data = article_iter.getLines().toList.tail
    .flatMap { row =>
      val splitted = row.split('\t')

      splitted.length match {
        case 4 =>
          Some(
            ArticleClinic(query = splitted(0).toLowerCase,
              frequency = splitted(1).toInt,
              fin_cluster_url = splitted(2),
              quantity = splitted(3).toInt
            ))
        case _ =>
          println(s"""Skip [article] row: "$row"""")
          None
      }
    }
    .groupBy(_.fin_cluster_url)
    .flatMap { case (_, rows) =>
      val sorted = rows.sortWith(_.frequency > _.frequency)
      val all = sorted.head.quantity
      findQuantity(all, sorted)
    }
    .filterNot(_.quantity == 0)
    .foreach { case x@ArticleClinic(query, frequency, fin_cluster_url, quantity) =>
      bookData.find(_.query == query) match {
        case Some(BookData(_, _, _, _, excep, domain, _, google_position)) =>
          pwArticle.println(
            s"""${query}\t${frequency}\t${fin_cluster_url}\t${excep}\t${domain}\t${quantity}\t${google_position}""")
        case None =>
          pwArticle.println(
            s"""${query}\t${frequency}\t${fin_cluster_url}\t\t"https://ru.bookimed.com"\t${quantity}\t15""")
      }
    }

  val pwClinic = new PrintWriter("/home/heroys6/archieve/BK-620/separate/outp-clinic.csv", "UTF-8")

  pwClinic.println("query\tfrequency\tfin_clust_url\texception\tdomain\tlinks_per_q\tposition")

  val clinic_iter = Source.fromFile("/home/heroys6/archieve/BK-620/clinic.tsv", "UTF-8")
  val clinic_data = clinic_iter.getLines().toList.tail
    .flatMap { row =>
      val splitted = row.split('\t')

      splitted.length match {
        case 4 =>
          //println("clinic_data.ArticleClinic.quantity: " + splitted(3).toInt)
          Some(
            ArticleClinic(query = splitted(0).toLowerCase,
              frequency = splitted(1).toInt,
              fin_cluster_url = splitted(2),
              quantity = splitted(3).toInt
            ))
        case _ =>
          println(s"""Skip [article] row: "$row"""")
          None
      }
    }
    .groupBy(_.fin_cluster_url)
    .flatMap { case (_, rows) =>
      val sorted = rows.sortWith(_.frequency > _.frequency)
      val all = sorted.head.quantity
      findQuantity(all, sorted)
    }
    .filterNot(_.quantity == 0)
    .foreach { case x@ArticleClinic(query, frequency, fin_cluster_url, quantity) =>
      bookData.find(_.query == query) match {
        case Some(BookData(_, _, _, _, excep, domain, _, google_position)) =>
          pwClinic.println(
            s"""${query}\t${frequency}\t${fin_cluster_url}\t${excep}\t${domain}\t${quantity}\t${google_position}""")
        case None =>
          pwClinic.println(
            s"""${query}\t${frequency}\t${fin_cluster_url}\t\t"https://ru.bookimed.com"\t${quantity}\t15""")
      }
    }

  pwClinic.close()
  pwArticle.close()

  println(s"""Done. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")

  /*import akka.stream.scaladsl.Source*/
}
