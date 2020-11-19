package cn.colorfulboxes.examples.spark.kudu

import cn.colorfulboxes.utils.spark.SparkUtil
import cn.colorfulboxes.utils.spark.config.DBconfig
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.JavaConverters._


/**
 * https://kudu.apache.org/docs/developing.html#_kudu_integration_with_spark
 */
class KuduQuickstart {


  var kuduContext: KuduContext = _
  var spark: SparkSession = _
  var sc: SparkContext = _

  @BeforeEach
  def createContext() = {
    spark = SparkUtil.getSpark(this.getClass.getSimpleName)
    sc = spark.sparkContext
    kuduContext = new KuduContext(DBconfig.getInstance.getKudu.url, sc)
  }

  @Test
  def createTable(): Unit = {
    // impala表名
    // println(kuduContext.tableExists("impala::POC_TEST.sdc"))

    // Delete the table if it already exists.
    if (kuduContext.tableExists("house_kudu.people")) {
      kuduContext.deleteTable("house_kudu.people")
    }

    val tableSchema: StructType = new StructType()
      .add("name", StringType, nullable = false)
      .add("age", IntegerType, nullable = false)
      .add("gpa", DoubleType, nullable = false)

    val keys = Seq("name")

    val options = new CreateTableOptions()
      .setNumReplicas(7)
      .addHashPartitions(List("name").asJava, 4)

    kuduContext.createTable("house_kudu.people", tableSchema, keys, options)
  }

  @Test
  def upsert() = {
    val peoples = for (i <- Range(0, 1000)) yield People(s"name$i", i % 50, i * 100)
    val peoplesRDD: RDD[People] = sc.parallelize(peoples)
    val peoplesDF = spark.createDataFrame(peoplesRDD)
    kuduContext.upsertRows(peoplesDF, "house_kudu.people")
  }

  @Test
  def delete() = {
    val peoples = for (i <- Range(0, 1000)) yield People(s"name$i", i % 50, i * 100)
    val peoplesRDD: RDD[People] = sc.parallelize(peoples)
    val peoplesDF = spark.createDataFrame(peoplesRDD)
    kuduContext.deleteRows(peoplesDF.select("name"), "house_kudu.people")

  }

  @AfterEach
  def closeContext() = {
    sc.stop()
    spark.stop()
  }
}

case class People(name: String, age: Int, gpa: Double)
