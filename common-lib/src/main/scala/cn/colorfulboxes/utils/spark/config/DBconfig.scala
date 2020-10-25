package cn.colorfulboxes.utils.spark.config

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

/**
 * 从配置文件加载DB配置
 */
object DBconfig {

  var dbObject: DBObject = dbObject

  private def getFromfactory: DBObject = {
    if (dbObject == null) dbObject = getDBProperties()
    dbObject
  }

  private def getDBProperties() = {
    val stream = getClass.getResourceAsStream("/db.yaml")
    val yaml = new Yaml(new Constructor(classOf[DBObject]))
    yaml.load(stream).asInstanceOf[DBObject]
  }

  def getInstance = getFromfactory
}
