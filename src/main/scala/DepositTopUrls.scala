import java.io.PrintWriter
import scala.io.Source

object DepositTopUrls extends App {

  case class DataRow(query: String, url: String, position: Int)

  val start = System.currentTimeMillis()
  val pw = new PrintWriter("/home/heroys6/Desktop/Oleg/outp-python-killer.csv", "UTF-8")

  pw.println("query\turl\tpos")

  // q url pos
  val iter = Source.fromFile("/home/heroys6/Desktop/Oleg/User_temp.csv", "UTF-8")
  val file = iter.getLines().toList

    file.map { row =>
      val splitted = row.split("\t")
      DataRow(splitted(0), splitted(1), splitted(2).toInt)
    }
    .groupBy(_.query)
    .map { top100 =>
      top100._2.sortWith(_.position < _.position)
    }
    .map { sorted =>
      sorted.find(_.url.contains("ru.depositphotos")) match {
        case Some(data) =>
          pw.println(s"${data.query}\t${data.url}\t${data.position}")
        case None =>
      }
    }
//    .map { _ =>
//      pw.close()
//      println(s"""Done. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
//    }

  pw.close()
  println(s"""Done. Time: ${(System.currentTimeMillis() - start).toFloat / 1000} sec""")
}
