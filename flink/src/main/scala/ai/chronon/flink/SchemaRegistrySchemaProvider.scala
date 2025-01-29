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

class SchemaRegistrySchemaProvider(conf: Map[String, String]) extends SchemaProvider(conf) {

  private val schemaRegistryUrl: String =
    conf.getOrElse("registry_url", throw new IllegalArgumentException("registry_url not set"))
  private val CacheCapacity: Int = 10

  private val schemaRegistryClient: SchemaRegistryClient = buildSchemaRegistryClient(schemaRegistryUrl)

  private[flink] def buildSchemaRegistryClient(registryUrl: String): SchemaRegistryClient =
    new CachedSchemaRegistryClient(registryUrl, CacheCapacity)

  override def buildEncoderAndDeserSchema(topicInfo: TopicInfo): (Encoder[Row], DeserializationSchema[Row]) = {
    val subject = topicInfo.params.getOrElse("subject", s"${topicInfo.name}-value")
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
