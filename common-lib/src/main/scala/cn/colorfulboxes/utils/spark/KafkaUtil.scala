package cn.colorfulboxes.utils.spark

import java.net.InetAddress

import cn.colorfulboxes.utils.spark.config.DBconfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

object KafkaUtil {

  //从kafka读数据
  def getKafkaStream(ssc: StreamingContext, groupId: String, offsetRanges: mutable.Map[TopicPartition, Long], topic: String, otherTopic: String*) = {
    val consumerParams = Map[String, Object](
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> DBconfig.getInstance.kafka.url,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean) // 手动提交
    )
    // kafka底层api，消费者直连kafka的leader分区
    // 直连方式：RDD的分区和kafka的分区一致
    KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, // 调度task到kafka所在节点
      ConsumerStrategies.Subscribe[String, String](otherTopic :+ topic, consumerParams, offsetRanges) // 指定消费topic
    )
  }

  def getProducerParams() = {
    Map[String, String](
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> DBconfig.getInstance.kafka.url,
      CommonClientConfigs.CLIENT_ID_CONFIG -> InetAddress.getLocalHost.getHostAddress,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName
    )
  }
}
