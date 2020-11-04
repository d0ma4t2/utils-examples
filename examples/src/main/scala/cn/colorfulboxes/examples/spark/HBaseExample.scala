package cn.colorfulboxes.examples.spark


import cn.colorfulboxes.utils.spark.{HBaseUtil, SparkUtil}
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Delete, Get, Increment, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions.GenericHBaseRDDFunctions
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * https://hbase.apache.org/book.html#_sparksqldataframes<br>
 * https://www.cnblogs.com/cssdongl/p/6238007.html<br>
 * http://support-it.huawei.com/docs/zh-cn/fusioninsight-all/fusioninsight_hd_6.5.1_documentation/zh-cn_topic_0165582918.html<br>
 * https://github.com/cloudera-labs/SparkOnHBase/tree/cdh5-0.0.2/src/main/scala/com/cloudera/spark/hbase/example
 */
object HBaseExample {

  private val log: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {

    val appName: String = this.getClass.getSimpleName

    val hc: HBaseContext = HBaseUtil.getHBaseContext(appName)
    //    val scan = new Scan
    //        scan.setCaching(100)
    //    scan.withStartRow(Bytes.toBytes(""))
    //    scan.withStopRow(Bytes.toBytes(""))

    val spark: SparkSession = SparkUtil.getSpark(appName)

    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val tableName = "fruit_spark"
    val columnFamily = "info"
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// 创建Hbase表
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //    val fruitTable = TableName.valueOf(tableName)
    //    val tableDescr: TableDescriptor = TableDescriptorBuilder.newBuilder(fruitTable)
    //      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily))
    //      .build()
    //
    //        val admin = HBaseUtil.getHBaseConn.getAdmin
    //    if (admin.tableExists(fruitTable)) {
    //      admin.disableTable(fruitTable)
    //      admin.deleteTable(fruitTable)
    //    }
    //    admin.createTable(tableDescr)
    //

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// BulkPut
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //    val rdd = sc.parallelize(Array(
    //      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("1")))),
    //      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("2")))),
    //      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("3")))),
    //      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("4")))),
    //      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("5")))),
    //      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("6")))),
    //      (Bytes.toBytes("7"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("7")))),
    //      (Bytes.toBytes("8"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("8")))),
    //      (Bytes.toBytes("9"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("9")))),
    //      (Bytes.toBytes("10"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("10"))))))
    //
    //    val timeStamp = System.currentTimeMillis()
    //
    //    // 插入hbase
    //    hc.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](
    //      rdd,
    //      TableName.valueOf(tableName),
    //      putRecord => {
    //        val put = new Put(putRecord._1) // rowkey
    //        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, timeStamp, putValue._3))
    //        put
    //      })

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// BulkGet
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //    try {
    //      val rdd: RDD[Array[Byte]] = sc.parallelize(Array(
    //        Bytes.toBytes("1"),
    //        Bytes.toBytes("2"),
    //        Bytes.toBytes("3"),
    //        Bytes.toBytes("4"),
    //        Bytes.toBytes("5"),
    //        Bytes.toBytes("6"),
    //        Bytes.toBytes("7")))
    //      val getRdd: RDD[String] = hc.bulkGet[Array[Byte], String](
    //        TableName.valueOf(tableName),
    //        2, // todo
    //        rdd,
    //        record => {
    //          log.info("making Get")
    //          new Get(record)
    //        },
    //        (result: Result) => {
    //          val it = result.listCells().iterator()
    //          val sb = new StringBuilder
    //          sb.append(Bytes.toString(result.getRow) + ":")
    //          while (it.hasNext) {
    //            val cell: Cell = it.next()
    //            val q = Bytes.toString(CellUtil.cloneQualifier(cell))
    //            if (q.equals("counter")) {
    //              sb.append("(" + q + "," + Bytes.toLong(CellUtil.cloneValue(cell)) + ")")
    //            } else {
    //              sb.append("(" + q + "," + Bytes.toString(CellUtil.cloneValue(cell)) + ")")
    //            }
    //          }
    //          sb.toString()
    //        })
    //      getRdd.collect().foreach(println)
    //    } finally {
    //      sc.stop()
    //    HBaseUtil.closeHbaseConn()
    //    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// BulkDelete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //    try {
    //      val rdd = sc.parallelize(Array(
    //        Bytes.toBytes("1"),
    //        Bytes.toBytes("2"),
    //        Bytes.toBytes("3"),
    //        Bytes.toBytes("4"),
    //        Bytes.toBytes("5")
    //      ))
    //      hc.bulkDelete[Array[Byte]](rdd,
    //        TableName.valueOf(tableName),
    //        putRecord => new Delete(putRecord),
    //        4)
    //    } finally {
    //      sc.stop()
    //    HBaseUtil.closeHbaseConn()
    //    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// BulkLoad
    /// hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles {hfilePath} {tableName}
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //    try {
    //      val arr = Array(
    //        "1," + columnFamily + ",b,1",
    //        "2," + columnFamily + ",a,2",
    //        "3," + columnFamily + ",b,1",
    //        "3," + columnFamily + ",a,1",
    //        "4," + columnFamily + ",a,3",
    //        "5," + columnFamily + ",b,3")
    //      val rdd = sc.parallelize(arr)
    //      hc.bulkLoad[String](
    //        rdd,
    //        TableName.valueOf(tableName),
    //        (putRecord) => {
    //          if (putRecord.length > 0) {
    //            val strArray = putRecord.split(",")
    //            val kfq = new KeyFamilyQualifier(Bytes.toBytes(strArray(0)), Bytes.toBytes(strArray(1)), Bytes.toBytes(strArray(2)))
    //            val pair = new Pair[KeyFamilyQualifier, Array[Byte]](kfq, Bytes.toBytes(strArray(3)))
    //            val ite = (kfq, Bytes.toBytes(strArray(3)))
    //            val itea = List(ite).iterator
    //            itea
    //          } else {
    //            null
    //          }
    //        },
    //        "file:\\D:\\project\\DevDemo\\utils-examples\\outputPath")// hdfs path
    //    } finally {
    //      sc.stop()
    //    HBaseUtil.closeHbaseConn()
    //    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// ForEachPartition
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //    try {
    //      val rdd = sc.parallelize(Array(
    //        (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("1")))),
    //        (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("2")))),
    //        (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("3")))),
    //        (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("4")))),
    //        (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("5"))))
    //      ),2)
    //      rdd.hbaseForeachPartition(
    //        hc,
    //        (it, connection) => {
    //          val m = connection.getBufferedMutator(TableName.valueOf(tableName))
    //          it.foreach(r => {
    //            val put = new Put(r._1)
    //            r._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
    //            m.mutate(put)
    //          })
    //          m.flush()
    //          m.close()
    //        })
    //    } finally {
    //      sc.stop()
    //    HBaseUtil.closeHbaseConn()
    //    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// DistributedScan
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //    try {
    //      val scan = new Scan()
    //      scan.setCaching(100)
    //      val getRdd: RDD[(ImmutableBytesWritable, Result)] = hc.hbaseRDD(TableName.valueOf(tableName), scan)
    //      getRdd.foreach(v => println(Bytes.toString(v._1.get())))
    //      println("Length: " + getRdd.map(r => r._1.copyBytes()).collect().length);
    //    } finally {
    //      sc.stop()
    //    HBaseUtil.closeHbaseConn()
    //    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// mapPartition接口并行遍历HBase
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // todo
    //    try {
    //      val rdd = sc.parallelize(Array(
    //        Bytes.toBytes("6"),
    //        Bytes.toBytes("7"),
    //        Bytes.toBytes("8"),
    //        Bytes.toBytes("9"),
    //        Bytes.toBytes("10")))
    //      val sb = new StringBuilder
    //      val getRdd = rdd.hbaseMapPartitions[String](
    //        hc,
    //        (it, connection) => {
    //          val table = connection.getTable(TableName.valueOf(tableName))
    //          it.map {
    //            r =>
    //              //batching would be faster.  This is just an example
    //              val result = table.get(new Get(r))
    //              val it = result.listCells().iterator()
    //              sb.append(Bytes.toString(result.getRow) + ":")
    //              while (it.hasNext) {
    //                val cell = it.next()
    //                val q = Bytes.toString(cell.getQualifierArray)
    //                if (q.equals("counter")) {
    //                  sb.append("(" + q + "," + Bytes.toLong(cell.getValueArray) + ")")
    //                } else {
    //                  sb.append("(" + q + "," + Bytes.toString(cell.getValueArray) + ")")
    //                }
    //              }
    //              sb.toString()
    //          }
    //        })
    //      getRdd.collect().foreach(v => println(v))
    //    } finally {
    //      sc.stop()
    //      HBaseUtil.closeHbaseConn()
    //    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// streamBulkPut
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //    try {
    //      val ssc = new StreamingContext(sc, Seconds(1))
    //      val lines = ssc.socketTextStream("localhost", 9999)
    //      hc.streamBulkPut[String](
    //        lines,
    //        TableName.valueOf(tableName),
    //        (putRecord) => {
    //          if (putRecord.length() > 0) {
    //            val put = new Put(Bytes.toBytes(putRecord))
    //            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("foo"), Bytes.toBytes("bar"))
    //            put
    //          } else {
    //            null
    //          }
    //        })
    //      ssc.start()
    //      ssc.awaitTerminationOrTimeout(60000)
    //      ssc.stop(stopSparkContext = false)
    //    } finally {
    //      sc.stop()
    //      HBaseUtil.closeHbaseConn()
    //    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// bulkIncrement
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // todo
    //    val rdd = sc.parallelize(
    //      Array(
    //        (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 1L))),
    //        (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 2L))),
    //        (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 3L))),
    //        (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 4L))),
    //        (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 5L)))
    //      )
    //    )
    //    hc.bulkIncrement[(Array[Byte], Array[(Array[Byte], Array[Byte], Long)])](rdd,
    //      tableName,
    //      (incrementRecord) => {
    //        val increment = new Increment(incrementRecord._1)
    //        incrementRecord._2.foreach((incrementValue) =>
    //          increment.addColumn(incrementValue._1, incrementValue._2, incrementValue._3))
    //        increment
    //      },
    //      4)

  }
}
