package cn.colorfulboxes.utils.spark

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
 * Kafka生产者单例（惰性）
 */
object KafkaProducerSingle {

  @volatile private var instance: Broadcast[KafkaSink[String, String]] = null

  def getInstance(sc: SparkContext): Broadcast[KafkaSink[String, String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          //将生产者广播
          instance = sc.broadcast(KafkaSink[String, String](KafkaUtil.getProducerParams()))
          instance
        }
      }
    }
    instance
  }
}