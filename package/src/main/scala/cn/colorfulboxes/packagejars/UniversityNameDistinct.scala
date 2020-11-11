package cn.colorfulboxes.packagejars

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks.{break, breakable}

/**
 * sudo -u hdfs spark-submit \
 * --master yarn \
 * --jars /opt/software/mysql-connector-java-5.1.49.jar \
 * --executor-memory 4G \
 * --total-executor-cores 8 \
 * --class cn.colorfulboxes.packagejars.UniversityNameDistinct /opt/spark_jars/universityNameDistinct.jar
 */
object UniversityNameDistinct {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val prop = new Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "unnet@2020#server")


    val array: Array[String] = spark.read.jdbc("jdbc:mysql://172.23.27.219/lianjia", "university_dict", prop)
      .map(e => e.getString(1)).collect()

    val bc: Broadcast[Array[String]] = spark.sparkContext.broadcast(array)

    spark.table("house.dwd_education_inf")
      .mapPartitions(it => {
        it.map(e => {
          val names: Array[String] = bc.value
          val tmpName: String = e.getAs[String]("university_school_name")
          var university_school_name = ""
          breakable {
            for (elem <- names) {
              if (tmpName.contains(elem)) {
                university_school_name = elem
                break
              }
            }
          }
          val community_id = e.getAs[String]("community_id")
          val name = e.getAs[String]("name")
          val area = e.getAs[String]("area")
          val kindergarten_name = e.getAs[String]("kindergarten_name")
          val kindergarten_distance = e.getAs[Int]("kindergarten_distance")
          val kindergarten_address = e.getAs[String]("kindergarten_address")
          val primary_school_name = e.getAs[String]("primary_school_name")
          val primary_school_distance = e.getAs[Int]("primary_school_distance")
          val primary_school_address = e.getAs[String]("primary_school_address")
          val secondary_school_name = e.getAs[String]("secondary_school_name")
          val secondary_school_distance = e.getAs[Int]("secondary_school_distance")
          val secondary_school_address = e.getAs[String]("secondary_school_address")
          val university_school_distance = e.getAs[Int]("university_school_distance")
          val university_school_address = e.getAs[String]("university_school_address")
          (community_id, name, area, kindergarten_name, kindergarten_distance, kindergarten_address, primary_school_name, primary_school_distance, primary_school_address, secondary_school_name, secondary_school_distance, secondary_school_address, university_school_name, university_school_distance, university_school_address)
        })
      }).toDF("community_id", "name", "area", "kindergarten_name", "kindergarten_distance", "kindergarten_address", "primary_school_name", "primary_school_distance", "primary_school_address", "secondary_school_name", "secondary_school_distance", "secondary_school_address", "university_school_name", "university_school_distance", "university_school_address")
      .write
      .mode("overwrite")
      .saveAsTable("house.dwd_education_info")
  }
}
