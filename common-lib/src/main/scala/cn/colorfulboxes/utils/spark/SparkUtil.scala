package cn.colorfulboxes.utils.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkUtil {
  def getSpark(appName: String, isLocal: Boolean = true, enableHive: Boolean = false) = {
    val builder: SparkSession.Builder = SparkSession.builder().appName(appName)
    if (isLocal) builder.master("local[*]")
    if (enableHive) builder.enableHiveSupport()
    builder.getOrCreate()
  }

  def getSparkContext(sparkSession: SparkSession) = {
    sparkSession.sparkContext
  }

  def getSparkConf(sparkContext: SparkContext) = {
    sparkContext.getConf
  }

  def getSparkConf(sparkSession: SparkSession) = {
    getSparkContext(sparkSession).getConf
  }
}
