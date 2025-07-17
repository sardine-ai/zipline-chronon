package ai.chronon.flink

import ai.chronon.online.TopicInfo

object FlinkUtils {

  def getProperty(key: String, props: Map[String, String], topicInfo: TopicInfo): Option[String] = {
    props
      .get(key)
      .filter(_.nonEmpty)
      .orElse {
        topicInfo.params.get(key)
      }
  }
}
