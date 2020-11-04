import scala.collection.mutable

object SqlBatchTest {
  def main(args: Array[String]): Unit = {
    val sql =
      """
        |INSERT INTO t_wordcount (word, counts) VALUES (%s, %d)
        |ON DUPLICATE KEY UPDATE counts = counts + %d
        |""".stripMargin
    var inserts = mutable.Buffer[String]()
    val res = Array(("1", 2, 3), ("2", 4, 5))
    for (elem <- res) {
      inserts += sql.format(elem._1, elem._2, elem._2)
    }

    inserts.foreach(println)
  }

}
