package cn.colorfulboxes.utils.spark

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable
import scala.collection.parallel.immutable

class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {

  //避免运行时产生NotSerializableException异常
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] = {
    //写入Kafka
    producer.send(new ProducerRecord[K, V](topic, key, value))
  }

  def send(topic: String, value: V): Future[RecordMetadata] = {
    //写入Kafka
    producer.send(new ProducerRecord[K, V](topic, value))
  }

  def send(topic: String, partition: Integer, key: K, value: V): Future[RecordMetadata] = {
    //写入Kafka
    producer.send(new ProducerRecord[K, V](topic, partition, key, value))
  }
}

object KafkaSink {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, String]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      //新建KafkaProducer
      val producer = new KafkaProducer[K, V](config)
      //虚拟机JVM退出时执行函数
      sys.addShutdownHook({
        //确保在Executor的JVM关闭前，KafkaProducer将缓存中的所有信息写入Kafka
        //close()会被阻塞直到之前所有发送的请求完成
        producer.close()
      })
      producer
    }
    new KafkaSink[K, V](createProducerFunc)
  }

  def apply[K, V](config: Properties): KafkaSink[K, V] = apply(config.toMap)
}