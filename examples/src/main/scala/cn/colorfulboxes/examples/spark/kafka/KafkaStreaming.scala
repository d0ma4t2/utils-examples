package cn.colorfulboxes.examples.spark.kafka

import cn.colorfulboxes.utils.spark.{KafkaUtil, RedisUtil}
import io.lettuce.core.api.sync.RedisCommands
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreaming {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val sc: SparkContext = session.sparkContext

    sc.setLogLevel(Level.WARN.toString)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    // kafka底层api，消费者直连kafka的leader分区
    // 直连方式：RDD的分区和kafka的分区一致
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(ssc, "g001", null, "") // 指定消费topic

    // 调用完createDirectStream直接在kafkaDStream调用foreachRDD，只有kafkaRDD中有偏移量!
    kafkaDStream.foreachRDD(rdd => {
      // 不会自动提交job
      if (!rdd.isEmpty()) {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //      offsetRanges.foreach(e => {
        //        println(s"topic: ${e.topic}, partition: ${e.partition}, formoffset: ${e.fromOffset}, untilOffset :${e.untilOffset}")
        //
        //      })
        val reduced: RDD[(String, Int)] = rdd.map(_.value())
          .flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _)

        reduced.foreachPartition(it => {
          // 获取redis
          val client: RedisCommands[String, String] = RedisUtil.redisCommands
          it.foreach(t => {
            client.hincrby("wc_adv", t._1, t._2)
          })
        })
        // 再更新这个批次每个分区的偏移量
        // 异步将偏移量写入Kafka
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
