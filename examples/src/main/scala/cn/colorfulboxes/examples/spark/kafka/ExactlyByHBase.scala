//package cn.colorfulboxes.examples.spark.kafka
//
//import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
//import java.util
//
//import cn.colorfulboxes.utils.spark.{HBaseUtil, KafkaUtil, SparkUtil}
//import com.alibaba.fastjson.JSON
//import org.apache.hadoop.hbase.TableName
//import org.apache.hadoop.hbase.client.{Put, Table}
//import org.apache.hadoop.hbase.spark.HBaseContext
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.TopicPartition
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.log4j.Level
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkContext, TaskContext}
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
//
//import scala.collection.mutable
//
//object ExactlyByHBase {
//
//  def main(args: Array[String]): Unit = {
//
//    val appName: String = this.getClass.getSimpleName
//    val groupId: String = args(0)
//
//    val sc: SparkContext = SparkUtil.getSpark(appName).sparkContext
//
//    sc.setLogLevel(Level.WARN.toString)
//
//    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
//
//    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:dn101:2181,nn102:2181,nn103:2181")
//    val ps0: PreparedStatement = conn.prepareStatement(
//      """
//        |SELECT topic_partition,
//        |       max(offset)
//        |FROM myorder
//        |WHERE groupid = ?
//        |GROUP BY topic_partition
//        |""".stripMargin)
//
//    val hc: HBaseContext = HBaseUtil.getHBaseContext(this.getClass.getSimpleName)
//
//
//    // 读取上一次的偏移量
//    ps0.setString(1, groupId)
//    val rs: ResultSet = ps0.executeQuery()
//    val map = mutable.Map[TopicPartition, Long]()
//    while (rs.next()) {
//      val topicAndPartition: String = rs.getString(1)
//      val offset: Long = rs.getLong(2)
//      val fields: Array[String] = topicAndPartition.split("_")
//      map += new TopicPartition(fields(0), fields(1).toInt) -> offset
//    }
//
//    // kafka底层api，消费者直连kafka的leader分区
//    // 直连方式：RDD的分区和kafka的分区一致
//    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(ssc, "groupId", map, "", "")
//
//    // 调用完createDirectStream直接在kafkaDStream调用foreachRDD，只有kafkaRDD中有偏移量!
//    kafkaDStream.foreachRDD(rdd => {
//      // 不会自动提交job
//      if (!rdd.isEmpty()) {
//        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        rdd.map(_.value())
//          .map(JSON.parseObject(_, classOf[Order]))
//          .filter(_ != null)
//          .foreachPartition(it => {
//            if(it.nonEmpty){
//              val offsetRange: OffsetRange = offsetRanges(TaskContext.get().partitionId())
//
//              val connection = HbaseUtil.getHBaseConn
//              val t_orders: Table = connection.getTable(TableName.valueOf("myorder"))
//              val puts = new util.LinkedList[Put]()
//              it.foreach(e => {
//                val put = new Put(Bytes.toBytes(e.oid))
////                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("groupid"), Bytes.toBytes(e.oid))
//                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("total_money"), Bytes.toBytes(e.totalMoney))
//
//                if(!it.hasNext){
//                  put.addColumn(Bytes.toBytes("offset"), Bytes.toBytes("groupid"), Bytes.toBytes(groupId))
//                  put.addColumn(Bytes.toBytes("offset"), Bytes.toBytes("total_partition"), Bytes.toBytes(offsetRange.topic+"_"+offsetRange.partition))
//                  put.addColumn(Bytes.toBytes("offset"), Bytes.toBytes("offset"), Bytes.toBytes(offsetRange.untilOffset))
//                }
//
//                puts.add(put)
//
//                if(puts.size() % 5 == 0){
//                  t_orders.put(put)
//                  puts.clear()
//                }
//              })
//              t_orders.put(puts)
//              t_orders.close()
//              connection.close()
//            }
//          })
//      }
//    })
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//  case class Order(oid: String, totalMoney: Double)
//
//}
