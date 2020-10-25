package cn.colorfulboxes.examples.spark.kafka

import java.util

import cn.colorfulboxes.utils.spark.{KafkaUtil, RedisUtil, SparkUtil}
import io.lettuce.core.api.sync.RedisCommands
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object ExactlyByRedisPipeline {

  def main(args: Array[String]): Unit = {

    val appName = this.getClass.getSimpleName
    val groupId = "g006"

    val sc: SparkContext = SparkUtil.getSpark(appName).sparkContext

    sc.setLogLevel(Level.WARN.toString)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    // 读取上一次的偏移量
    val map = mutable.Map[TopicPartition, Long]()
    val redisCommands: RedisCommands[String, String] = RedisUtil.redisCommands
    val topicAndPartition: util.Map[String, String] = redisCommands.hgetall(appName + "_" + groupId)

    import scala.collection.JavaConverters._

    topicAndPartition.asScala.foreach(e => {
      val fields: Array[String] = e._1.split("_")
      map += new TopicPartition(fields(0), fields(1).toInt) -> e._2.toLong
    })

    // kafka底层api，消费者直连kafka的leader分区
    // 直连方式：RDD的分区和kafka的分区一致
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(ssc, groupId, map, "", "") // 指定消费topic

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
          redisCommands.multi()
          for (elem <- res) {
            redisCommands.hincrby("WORD_COUNT", elem._1, elem._2)
          }

          for (elem <- offsetRanges) {
            redisCommands.hset(appName + "_" + groupId, elem.topic + "_" + elem.partition, elem.untilOffset.toString)
          }
          redisCommands.exec()
        } catch {
          case e: Exception => {
            redisCommands.discard()
            ssc.stop()
          }
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
