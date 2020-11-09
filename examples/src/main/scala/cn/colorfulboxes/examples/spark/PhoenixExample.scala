package cn.colorfulboxes.examples.spark

;

import java.util
import java.util.Properties

import cn.colorfulboxes.utils.spark.SparkUtil
import cn.colorfulboxes.utils.spark.config.{DBconfig, Phoenix}
import cn.hutool.db.{Db, Entity}
import cn.hutool.db.ds.pooled.{DbConfig, PooledDataSource}
import io.netty.handler.codec.http2.Http2FrameReader.Configuration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.spark.internal.config
import scala.collection.JavaConverters._

/**
 * https://cloud.tencent.com/developer/news/610010
 * es + hbase = best
 */

object PhoenixExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSpark(this.getClass.getSimpleName)


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// df
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //        val conf = HBaseConfiguration.create()
    //        conf.addResource(new Path("D:\\project\\DevDemo\\utils-examples\\examples\\src\\main\\resources\\core-site.xml"))
    //        conf.addResource(new Path("D:\\project\\DevDemo\\utils-examples\\examples\\src\\main\\resources\\hbase-site.xml"))
    //        conf.addResource(new Path("D:\\project\\DevDemo\\utils-examples\\examples\\src\\main\\resources\\hdfs-site.xml"))

    import spark.implicits._
//    val phoenix: Phoenix = DBconfig.getInstance.getPhoenix
//    val properties = new Properties()
//    //    properties.setProperty("driver", phoenix.getDriver) hikari driver
//    properties.setProperty("jdbcUrl", phoenix.getUrl)
//    val dataSource = new HikariDataSource(new HikariConfig(properties))
    // com.zaxxer.hikari.pool.HikariPool$PoolInitializationException: Failed to initialize pool: ERROR 726 (43M10):  Inconsistent namespace mapping properties. Cannot initiate connection as SYSTEM:CATALOG is found but client does not have phoenix.schema.isNamespaceMappingEnabled enabled
//        Db.use(dataSource).execute(
//          """
//            |create table house.test(
//            | id integer not null primary key ,
//            | description varchar,
//            | xy varchar
//            |)
//            |""".stripMargin)

//    val list = Db.use(dataSource).query("select * from house.test where xy = '北京'".stripMargin).asScala
//
//    list.foreach(println)


    import spark.implicits._

    //    spark.sparkContext.parallelize(Array((192, "s4","北京"), (123, "s5","上海")))
    //      .toDF("id", "description","xy")
    //      .write
    //      .format("org.apache.phoenix.spark")
    //      .mode(SaveMode.Overwrite)
    //      .options(Map("table" -> "house.test", "zkUrl" -> "172.23.26.111,172.23.26.112,172.23.26.113:2181"))
    //      .save()

    //
//    spark.read
    //      .format("org.apache.phoenix.spark")
    //      .options(Map("table" -> "house.test", "zkUrl" -> "172.23.26.111,172.23.26.112,172.23.26.113:2181"))
    //      .load()
    //      .show()
    //
    //    spark.sql(
    //      """
    //        |select * from house.test where xy = '北京'
    //        |""".stripMargin)
    //      .show()



    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// jdbc
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //    import spark.implicits._
    // INTEGER cannot be coerced to VARCHAR
    //    spark.sparkContext.parallelize(Array(("192.168.0.8", ), ("192.168.0.7", 7)))
    //    spark.sparkContext.parallelize(Array(("192.168.0.8", "8"), ("192.168.0.9", "9")))
    //      .toDF("HOST", "DESCRIPTION")
    //      .write
    //      .format("org.apache.phoenix.spark")
    //      .mode(SaveMode.Overwrite)
    //      .options(Map("table" -> "TEST", "zkUrl" -> phoenix.getUrl))
    //      .save()
    //
    //    spark.read.jdbc(phoenix.getUrl, "TEST", new Properties())
    //      .toDF("HOST", "DESCRIPTION")
    //      .show()

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //        import spark.implicits._
            val phoenix: Phoenix = DBconfig.getInstance.getPhoenix
            val properties = new Properties()
            properties.setProperty("driver", phoenix.getDriver)
            properties.setProperty("jdbcUrl", phoenix.getUrl)
    //        val dataSource = new HikariDataSource(new HikariConfig(properties))
    //
    //    Db.use(dataSource).execute(
    //      """
    //        |create table test_phoenix_api(
    //        | mykey integer not null primary key ,
    //        | mycolumn varchar
    //        |)
    //        |""".stripMargin)
    //
    //    spark.sparkContext.parallelize(Array((3, "8"), (4, "9")))
    //      .toDF("MYKEY", "MYCOLUMN")
    //      .write
    //      .format("org.apache.phoenix.spark")
    //      .mode(SaveMode.Overwrite)
    //      .options(Map("table" -> "test_phoenix_api", "zkUrl" -> phoenix.getUrl))
    //      .save()
    //
            spark.read.jdbc(phoenix.getUrl, "house.person", properties)
              .toDF()
              .show()

  }
}
