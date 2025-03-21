package ai.chronon.flink

import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.GroupBy
import ai.chronon.online.TopicInfo
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.spark.sql.avro.{AvroDeserializationSupport, ProjectedAvroDeserializationSupport}

/** SchemaProvider that uses the Confluent Schema Registry to fetch schemas for topics.
  * Can be configured as: topic = "kafka://topic-name/registry_host=host/[registry_port=port]/[registry_scheme=http]/[subject=subject]"
  * Port, scheme and subject are optional. If port is missing, we assume the host is pointing to a LB address / such that
  * forwards to the right host + port. Scheme defaults to http. Subject defaults to the topic name + "-value" (based on schema
  * registry conventions).
  */
class ProjectedSchemaRegistrySchemaProvider(conf: Map[String, String]) extends ProjectedSchemaProvider(conf) {
  import SchemaRegistrySchemaProvider._

  private val schemaRegistryHost: String =
    conf.getOrElse(RegistryHostKey, throw new IllegalArgumentException(s"$RegistryHostKey not set"))

  // port is optional as many folks configure just the host as it's behind an LB
  private val schemaRegistryPortString: Option[String] = conf.get(RegistryPortKey)

  // default to http if not set
  private val schemaRegistrySchemeString: String = conf.getOrElse(RegistrySchemeKey, "http")

  private val CacheCapacity: Int = 10

  private val schemaRegistryClient: SchemaRegistryClient =
    buildSchemaRegistryClient(schemaRegistrySchemeString, schemaRegistryHost, schemaRegistryPortString)

  private[flink] def buildSchemaRegistryClient(schemeString: String,
                                               registryHost: String,
                                               maybePortString: Option[String]): SchemaRegistryClient = {
    maybePortString match {
      case Some(portString) =>
        val registryUrl = s"$schemeString://$registryHost:$portString"
        new CachedSchemaRegistryClient(registryUrl, CacheCapacity)
      case None =>
        val registryUrl = s"$schemeString://$registryHost"
        new CachedSchemaRegistryClient(registryUrl, CacheCapacity)
    }
  }

  override def buildProjectedSourceDeserializer(groupBy: GroupBy): ChrononDeserializationSchema = {
    val topicUri = groupBy.streamingSource.get.topic
    val topicInfo = TopicInfo.parse(topicUri)
    val subject = topicInfo.params.getOrElse(RegistrySubjectKey, s"${topicInfo.name}-value")
    val parsedSchema =
      try {
        val metadata = schemaRegistryClient.getLatestSchemaMetadata(subject)
        schemaRegistryClient.getSchemaById(metadata.getId)
      } catch {
        case e: RestClientException =>
          throw new IllegalArgumentException(
            s"Failed to retrieve schema details from the registry. Status: ${e.getStatus}; Error code: ${e.getErrorCode}",
            e)
        case e: Exception =>
          throw new IllegalArgumentException("Error connecting to and requesting schema details from the registry", e)
      }
    // we currently only support Avro encoders
    parsedSchema.schemaType() match {
      case AvroSchema.TYPE =>
        val schema = parsedSchema.asInstanceOf[AvroSchema]
        ProjectedAvroDeserializationSupport.build(groupBy: GroupBy, schema.canonicalString(), schemaRegistryWireFormat = true)
      case _ => throw new IllegalArgumentException(s"Unsupported schema type: ${parsedSchema.schemaType()}")
    }
  }
}
