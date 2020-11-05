package cn.colorfulboxes.utils.spark.config

import scala.beans.BeanProperty

class DBObject extends Serializable {
  @BeanProperty var spark: Spark = _
  @BeanProperty var flink: Flink = _
  @BeanProperty var mysql: Mysql = _
  @BeanProperty var phoenix: Phoenix = _
  @BeanProperty var redis: Redis = _
  @BeanProperty var elasticsearch: Elasticsearch = _
  @BeanProperty var hive: Hive = _
  @BeanProperty var hadoop: Hadoop = _
  @BeanProperty var hbase: HBase = _
  @BeanProperty var kafka: Kafka = _
  @BeanProperty var kudu: Kudu = _
  @BeanProperty var impala: Impala = _
}

class Spark extends Serializable{
  @BeanProperty var url: String = _
}

class Flink extends Serializable{
  @BeanProperty var url: String = _
}


class Mysql extends Serializable {
  @BeanProperty var driverClassName: String = _
  @BeanProperty var jdbcUrl: String = _
  @BeanProperty var username: String = _
  @BeanProperty var password: String = _
  @BeanProperty var maximumPoolSize: String = _
  @BeanProperty var minimumIdle: String = _
  @BeanProperty var idleTimeout: String = _
  @BeanProperty var connectionTimeout: String = _
  @BeanProperty var maxLifetime: String = _
  @BeanProperty var readOnly: String = _
}

class Phoenix extends Serializable {
  @BeanProperty var driver: String = _
  @BeanProperty var url: String = _
}

class Redis extends Serializable {
  @BeanProperty var url: String = _
}

class Elasticsearch extends Serializable {
  @BeanProperty var url: String = _
}

class HBase extends Serializable {
  @BeanProperty var url: String = _
}

class Kafka extends Serializable {
  @BeanProperty var url: String = _
}

class Hadoop extends Serializable {
  @BeanProperty var url: String = _
}

class Hive extends Serializable {
  @BeanProperty var url: String = _
}

class Kudu extends Serializable {
  @BeanProperty var url: String = _
}

class Impala extends Serializable {
  @BeanProperty var url: String = _
}
