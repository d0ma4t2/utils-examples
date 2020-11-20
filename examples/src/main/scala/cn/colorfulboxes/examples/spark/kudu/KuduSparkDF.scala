package cn.colorfulboxes.examples.spark.kudu

import cn.colorfulboxes.utils.spark.SparkUtil
import cn.colorfulboxes.utils.spark.config.DBconfig
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

class KuduSparkDF {

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
  def write() = {
    val peoples = for (i <- Range(1000, 2000)) yield People(s"name$i", i % 50, i * 100)
    val peoplesRDD: RDD[People] = sc.parallelize(peoples)

    spark.createDataFrame(peoplesRDD)
      .write
      .option("kudu.table","house_kudu.people")
      .option("kudu.master",DBconfig.getInstance.getKudu.url)
      .mode("append")
      .format("kudu")
      .save()
  }

  @Test
  def read() = {

    spark.read
      .option("kudu.table","house_kudu.people")
      .option("kudu.master",DBconfig.getInstance.getKudu.url)
      .format("kudu")
      .load
      .show(10)
  }

  @Test
  def sql() = {
    spark.read
      .option("kudu.table","house_kudu.people")
      .option("kudu.master",DBconfig.getInstance.getKudu.url)
      .format("kudu")
      .load()
      .createTempView("people")

    spark.sql(
      """
        |select name
        |from people
        |limit 10
        |""".stripMargin)
      .show(10)
  }

  @AfterEach
  def closeContext() = {
    sc.stop()
    spark.stop()
  }
}
