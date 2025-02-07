package ai.chronon.flink
import ai.chronon.online.TopicInfo
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro.AvroDeserializationSupport

/**
  * SchemaProvider that uses the Confluent Schema Registry to fetch schemas for topics.
  * Can be configured as: topic = "kafka://topic-name/registry_host=host/[registry_port=port]/[registry_scheme=http]/[subject=subject]"
  * Port, scheme and subject are optional. If port is missing, we assume the host is pointing to a LB address / such that
  * forwards to the right host + port. Scheme defaults to http. Subject defaults to the topic name + "-value" (based on schema
  * registry conventions).
  */
class SchemaRegistrySchemaProvider(conf: Map[String, String]) extends SchemaProvider(conf) {
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

  override def buildEncoderAndDeserSchema(topicInfo: TopicInfo): (Encoder[Row], DeserializationSchema[Row]) = {
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
        AvroDeserializationSupport.build(topicInfo.name, schema.canonicalString(), schemaRegistryWireFormat = true)
      case _ => throw new IllegalArgumentException(s"Unsupported schema type: ${parsedSchema.schemaType()}")
    }
  }
}

object SchemaRegistrySchemaProvider {
  val RegistryHostKey = "registry_host"
  val RegistryPortKey = "registry_port"
  val RegistrySchemeKey = "registry_scheme"
  val RegistrySubjectKey = "subject"
}
