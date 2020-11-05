package cn.colorfulboxes.examples.spark

;

import java.util.Properties

import cn.colorfulboxes.utils.spark.SparkUtil
import cn.colorfulboxes.utils.spark.config.{DBconfig, Phoenix}
import cn.hutool.db.Db
import cn.hutool.db.ds.pooled.{DbConfig, PooledDataSource}
import io.netty.handler.codec.http2.Http2FrameReader.Configuration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.spark.internal.config


object PhoenixExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSpark(this.getClass.getSimpleName)


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// df
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("D:\\project\\DevDemo\\utils-examples\\examples\\src\\main\\resources\\core-site.xml"))
    conf.addResource(new Path("D:\\project\\DevDemo\\utils-examples\\examples\\src\\main\\resources\\hbase-site.xml"))
    conf.addResource(new Path("D:\\project\\DevDemo\\utils-examples\\examples\\src\\main\\resources\\hdfs-site.xml"))

    //      spark.read
    //        .format("org.apache.phoenix.spark")
    //        .options(Map("table" -> "TEST", "zkUrl" -> "dn37,dn38,dn36:2181"))
    //        .load()
    //        .show()


    //    import spark.implicits._
    //
    //    spark.sparkContext.parallelize(Array(("192.168.0.4","s4"),("192.168.0.5","s5")))
    //      .toDF("HOST","DESCRIPTION")
    //      .write
    //      .format("org.apache.phoenix.spark")
    //      .mode(SaveMode.Overwrite)
    //      .options(Map("table" -> "TEST", "zkUrl" -> "dn37,dn38,dn36:2181"))
    //      .save()

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// jdbc
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    import spark.implicits._
    // INTEGER cannot be coerced to VARCHAR
    //    spark.sparkContext.parallelize(Array(("192.168.0.8", ), ("192.168.0.7", 7)))
//    spark.sparkContext.parallelize(Array(("192.168.0.8", "8"), ("192.168.0.9", "9")))
//      .toDF("HOST", "DESCRIPTION")
//      .write
//      .format("org.apache.phoenix.spark")
//      .mode(SaveMode.Overwrite)
//      .options(Map("table" -> "TEST", "zkUrl" -> "jdbc:phoenix:dn37,dn38,dn36:2181"))
//      .save()
//
//    spark.read.jdbc("jdbc:phoenix:dn37,dn38,dn36:2181", "TEST", new Properties())
//      .toDF("HOST", "DESCRIPTION")
//      .show()

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val phoenix: Phoenix = DBconfig.getInstance.getPhoenix
    val properties = new Properties()
    properties.setProperty("driverClassName", phoenix.getDriver)
    properties.setProperty("jdbcUrl", phoenix.getUrl)
    val dataSource = new HikariDataSource(new HikariConfig(properties))

//    Db.use(dataSource).execute(
//      """
//        |create table test_phoenix_api(
//        | mykey integer not null primary key ,
//        | mycolumn varchar
//        |)
//        |""".stripMargin)
    spark.sparkContext.parallelize(Array((3, "8"), (4, "9")))
      .toDF("MYKEY", "MYCOLUMN")
      .write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .options(Map("table" -> "test_phoenix_api", "zkUrl" -> "jdbc:phoenix:dn37,dn38,dn36:2181"))
      .save()

    spark.read.jdbc("jdbc:phoenix:dn37,dn38,dn36:2181", "test_phoenix_api", new Properties())
      .toDF()
      .show()

  }
}
