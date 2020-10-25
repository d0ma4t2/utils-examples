package cn.colorfulboxes.examples.spark.kafka

import cn.colorfulboxes.utils.spark.{KafkaUtil, RedisUtil}
import io.lettuce.core.api.sync.RedisCommands
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreamingAutocommit {

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


    // 获取redis
    val client: RedisCommands[String, String] = RedisUtil.redisCommands

    kafkaDStream.map(_.value)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreach(e => {
          client.hincrby("wordcount", e._1, e._2)
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
