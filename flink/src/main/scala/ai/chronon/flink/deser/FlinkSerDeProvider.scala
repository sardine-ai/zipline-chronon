package ai.chronon.flink.deser

import ai.chronon.flink.deser.CustomSchemaSerDe.ProviderClass
import ai.chronon.flink.deser.SchemaRegistrySerDe.RegistryHostKey
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.SerDe

// Provider for the relevant SerDe implementation for Flink.
object FlinkSerDeProvider {
  def build(topicInfo: TopicInfo): SerDe = {
    val maybeSchemaRegistryHost = topicInfo.params.get(RegistryHostKey)
    val maybeCustomProviderClass = topicInfo.params.get(ProviderClass)
    (maybeSchemaRegistryHost, maybeCustomProviderClass) match {
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException(
          s"Both $RegistryHostKey and $ProviderClass are set. Please set only one of them.")
      case (None, None) =>
        throw new IllegalArgumentException(
          s"Neither $RegistryHostKey nor $ProviderClass are set. Please set one of them.")
      case (Some(_), None) =>
        new SchemaRegistrySerDe(topicInfo)
      case (None, Some(_)) =>
        CustomSchemaSerDe.buildCustomSchemaSerDe(topicInfo)
    }
  }
}
