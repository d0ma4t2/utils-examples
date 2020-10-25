package cn.colorfulboxes.utils.spark.config.pool

import java.util.Properties

import cn.colorfulboxes.utils.spark.config.DBconfig
import cn.hutool.core.bean.BeanUtil
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object DataSourcePool {
  private val instance = DataSourcePool

  //    private static Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

  def getInstance = instance

  def getDataSource: HikariDataSource = {
    // 多数据源
    // if (dataSources.containsKey(key)) {
    //     return dataSources.get(key);
    // }
    val properties = new Properties
    BeanUtil.copyProperties(DBconfig.getInstance.mysql, properties)
    new HikariDataSource(new HikariConfig(properties))
    // dataSources.put(key, dataSource);

  }
}
