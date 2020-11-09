package cn.colorfulboxes.examples.spark.kudu

import cn.colorfulboxes.utils.spark.SparkUtil
import cn.colorfulboxes.utils.spark.config.DBconfig
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


/**
 * https://kudu.apache.org/docs/developing.html#_kudu_integration_with_spark
 */
object KuduQuickstart {

  // todo no test
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSpark(this.getClass.getSimpleName)

    val sc: SparkContext = spark.sparkContext
    val kuduContext = new KuduContext(DBconfig.getInstance.getKudu.url, sc)

    // Delete the table if it already exists.
    if(kuduContext.tableExists("tableName")) {
      kuduContext.deleteTable("tableName")
    }

    kuduContext.createTable("tableName", new StructType(), Seq("",""),
      new CreateTableOptions()
        .setNumReplicas(3)
        .addHashPartitions(List("").asJava, 4))


  }
}
