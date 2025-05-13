package ai.chronon.flink.deser

import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.SerDe

// Configured in topic config in this fashion:
// kafka://test-beacon-main/provider_class=ai.chronon.flink.deser.MockCustomSchemaProvider/schema_name=beacon
object CustomSchemaSerDe {
  val ProviderClass = "provider_class"
  val SchemaName = "schema_name"

  def buildCustomSchemaSerDe(topicInfo: TopicInfo): SerDe = {
    val cl = Thread.currentThread().getContextClassLoader // Use Flink's classloader
    val providerClass =
      topicInfo.params.getOrElse(ProviderClass, throw new IllegalArgumentException(s"$ProviderClass not set"))
    val cls = cl.loadClass(providerClass)
    val constructor = cls.getConstructors.apply(0)
    val provider = constructor.newInstance(topicInfo)
    provider.asInstanceOf[SerDe]
  }
}
