import java.io.PrintWriter

import scala.io.Source
import scala.util.Try
import SCfixWithLemmas._

object LowerCaseToNormal extends App {
  val start = System.currentTimeMillis()

  val clinicHeader = "Anchor\tAcceptor\tDonor\tPosition\tFreq"
  val queryTailMapClinic = Source.fromFile("/home/heroys6/archieve/BK-624/Inp/relinker_clinics.tsv", "UTF-8")
    .getLines().toList.tail
    .map { row =>
      val splitted = row.split('\t')

      if (Try(splitted(0)).isSuccess) {
        (splitted(0).toLowerCase, "\t" + splitted.tail.mkString("\t"))
      }
      else throw new Exception("Error on clinic: " + splitted)
    }

  val pwClinic = new PrintWriter("/home/heroys6/archieve/BK-624/relinker_clinics_BigLet.tsv", "UTF-8")

  val lowBigMapClinic = Source.fromFile("/home/heroys6/archieve/BK-624/Inp/clinic.tsv")
    .getLines().toList.tail
    .map { row =>
      val splitted = row.split('\t')

      if (Try(splitted(0)).isSuccess) {
        (splitted(0).toLowerCase, splitted(0))
      }
      else throw new Exception("Error on BigLet clinic: " + splitted)
    }
    .toMap

  pwClinic.println(clinicHeader)

  queryTailMapClinic
    .map( x => firstBig(lowBigMapClinic(x._1)) + x._2)
    .map(pwClinic.println)

  pwClinic.close()

  val articleHeader = "Anchor\tAcceptor\tDonor\tPosition\tFreq"
  val queryTailListArticle = Source.fromFile("/home/heroys6/archieve/BK-624/Inp/relinker_articles_last.tsv", "UTF-8")
    .getLines().toList.tail
    .map { row =>
      val splitted = row.split('\t')

      if (Try(splitted(0)).isSuccess) {
        (splitted(0).toLowerCase, "\t" + splitted.tail.mkString("\t"))
      }
      else throw new Exception("Error on article: " + splitted)
    }

  val pwArticle = new PrintWriter("/home/heroys6/archieve/BK-624/relinker_articles_BigLet.tsv", "UTF-8")

  val lowBigMapArticle = Source.fromFile("/home/heroys6/archieve/BK-624/Inp/article.tsv")
    .getLines().toList.tail
    .map { row =>
      val splitted = row.split('\t')

      if (Try(splitted(0)).isSuccess) {
        (splitted(0).toLowerCase, splitted(0))
      }
      else throw new Exception("Error on BigLet article: " + splitted)
    }
    .toMap

  pwArticle.println(articleHeader)

  /*var i = 0
  var clinics = 0
  var clinic = 0*/

  queryTailListArticle
    .map(x => firstBig(lowBigMapArticle(x._1)) + x._2)
    .map(pwArticle.println)
    /*.map(x =>
      Try(lowBigMapArticle(x._1)).getOrElse {
        i = i + 1
        if (x._2.split('\t')(0).contains("/clinics/")) clinics = clinics + 1
        else if (x._2.split('\t')(0).contains("/clinic/")) clinic = clinic + 1
        else println(s"In article.tsv no query: ${x}") /*println(s"${x._2.split('\t')}")*/
      })*/

  pwArticle.close()

  /*println("Quantity: " + i)
  println("Quantity with /clinics/: " + clinics)
  println("Quantity with /clinic/: " + clinic)*/

  println(s"""Done. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} secs""")
}