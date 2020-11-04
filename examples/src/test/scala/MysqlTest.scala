import cn.hutool.db.{Db, Entity}

object MysqlTest {

  def main(args: Array[String]): Unit = {

    import scala.collection.JavaConverters._
    val offsetses = Db.use().find(
      List("topic_partition", "offset").asJava,
      Entity.create("t_kafka_offset").set("app_gid", "1")
    ).asScala

    offsetses.foreach(e => println(e.getStr("topic_partition"), e.getLong("offset")))
  }
}
