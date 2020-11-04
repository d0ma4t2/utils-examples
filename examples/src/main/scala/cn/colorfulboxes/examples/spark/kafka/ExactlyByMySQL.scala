package cn.colorfulboxes.examples.spark.kafka

import java.sql.SQLException

import cn.colorfulboxes.utils.spark.config.pool.DataSourcePool
import cn.colorfulboxes.utils.spark.{KafkaUtil, SparkUtil}
import cn.hutool.db.{Db, Entity, Session}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object ExactlyByMySQL {

  def main(args: Array[String]): Unit = {

    val appName = this.getClass.getSimpleName
    val groupId = args(0)
    val sc: SparkContext = SparkUtil.getSpark(appName).sparkContext

    sc.setLogLevel(Level.WARN.toString)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    import scala.collection.JavaConverters._

    val offsetses = Db.use(DataSourcePool.getInstance.getDataSource).find(
      List("topic_partition", "offset").asJava,
      Entity.create("t_kafka_offset").set("app_gid", appName + "_" + groupId)
    ).asScala

    // 读取上一次的偏移量

    val map = mutable.Map[TopicPartition, Long]()
    for (e <- offsetses) {
      val fields: Array[String] = e.getStr("topic_partition").split("_")
      map += new TopicPartition(fields(0), fields(1).toInt) -> e.getLong("offset")
    }

    // kafka底层api，消费者直连kafka的leader分区
    // 直连方式：RDD的分区和kafka的分区一致
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(ssc, "groupId", map, "", "")

    val session = Session.create
    // 调用完createDirectStream直接在kafkaDStream调用foreachRDD，只有kafkaRDD中有偏移量!
    kafkaDStream.foreachRDD(rdd => {
      // 不会自动提交job
      if (!rdd.isEmpty()) {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val reduced: RDD[(String, Int)] = rdd.map(_.value())
          .flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _)

        // 只是用于聚合类任务
        val res: Array[(String, Int)] = reduced.collect()

        try {
          session.beginTransaction()
          val sql1 =
            """
              |INSERT INTO t_wordcount (word, counts) VALUES (%s, %d)
              |ON DUPLICATE KEY UPDATE counts = counts + %d
              |""".stripMargin
          val inserts1 = mutable.Buffer[String]()
          for (elem <- res) {
            inserts1 += sql1.format(elem._1, elem._2, elem._2)
          }

          session.executeBatch(inserts1.asJava)


          val inserts2 = mutable.Buffer[String]()
          val sql2 =
            """
              |INSERT INTO t_kafka_offset (app_gid, topic_partition, offset) VALUES (%s, %s, %d)
              |ON DUPLICATE KEY UPDATE offset = %d""".stripMargin
          for (elem <- offsetRanges) {
            inserts2 += sql2.format(appName + groupId, elem.topic + "_" + elem.partition, elem.untilOffset, elem.untilOffset)
          }

          session.executeBatch(inserts2.asJava)

          session.commit()

        } catch {
          case e: SQLException => {
            e.printStackTrace()
            session.quietRollback()
            ssc.stop()
          }
        } finally {

        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
