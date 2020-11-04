package cn.colorfulboxes.utils.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext

object HBaseUtil {

  private var sc: SparkContext = _

  val conf: Configuration = init()


  private var hbaseContext: HBaseContext = _

  //HBase连接
  @volatile private var connection: Connection = _
  //请求的连接数计数器（为0时关闭）
  @volatile private var num = 0

  private def init() = {
    val config: Configuration = HBaseConfiguration.create()
    config.addResource(new Path("D:\\project\\DevDemo\\utils-examples\\examples\\src\\main\\resources\\core-site.xml"))
    config.addResource(new Path("D:\\project\\DevDemo\\utils-examples\\examples\\src\\main\\resources\\hbase-site.xml"))
    config
  }

  def getHBaseContext(name: String) = {

    if (sc == null) sc = SparkUtil.getSpark(name).sparkContext
    if (hbaseContext == null) hbaseContext = new HBaseContext(sc, conf)
    hbaseContext
  }

  def getSpark(name: String) = {
    if (sc == null) sc = SparkUtil.getSpark(name).sparkContext
    sc
  }

  //获取HBase连接
  def getHBaseConn: Connection = {
    synchronized {
      if (connection == null || connection.isClosed() || num == 0) {
        connection = ConnectionFactory.createConnection(conf)
      }
      //每请求一次连接，计数器加一
      num = num + 1
    }
    connection
  }

  //关闭HBase连接
  def closeHbaseConn(): Unit = {
    synchronized {
      if (num <= 0) {
        println("no conn to close!")
        return
      }
      //每请求一次关闭连接，计数器减一
      num = num - 1
      //请求连接计数器为0时关闭连接
      if (num == 0 && connection != null && !connection.isClosed()) {
        connection.close()
      }
    }
  }

}
