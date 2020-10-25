package cn.colorfulboxes.utils.spark.config.model

case class TopicOffsets(topic_partition: String,
                        offset: Long)