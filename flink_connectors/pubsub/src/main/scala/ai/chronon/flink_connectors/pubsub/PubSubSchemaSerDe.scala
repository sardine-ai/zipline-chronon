package ai.chronon.flink_connectors.pubsub

import ai.chronon.api.StructType
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroCodec, AvroSerDe, Mutation, ProtobufSerDe, SerDe}
import com.google.api.gax.rpc.NotFoundException
import com.google.cloud.pubsub.v1.SchemaServiceClient
import com.google.pubsub.v1.ListSchemaRevisionsRequest;
import com.google.pubsub.v1.SchemaName
import com.google.pubsub.v1.SchemaView;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema

import scala.jdk.CollectionConverters._

/** SerDe that fetches schemas from GCP Pub/Sub Schema Registry and auto-detects the format (Avro or Protobuf).
  *
  * Configure via topic string:
  *   pubsub://topic-name/serde=pubsub_schema/project=my-project/schemaId=my-schema/[schemaRevisionId=revision-id]/[proto3_default_as_null=false]
  *
  * Parameters:
  *   - project: GCP project name (required)
  *   - schemaId: Schema ID in Pub/Sub Schema Registry (required)
  *   - schemaRevisionId: Specific schema revision ID to use (optional). If omitted, uses the latest revision.
  *   - proto3_default_as_null: For protobuf schemas, treat proto3 default values as null (optional, defaults to false)
  */
class PubSubSchemaSerDe(topicInfo: TopicInfo) extends SerDe {
  import PubSubSchemaSerDe._

  private val proto3DefaultAsNull: Boolean =
    topicInfo.params.getOrElse(Proto3DefaultAsNullKey, "false").toBoolean

  protected[flink_connectors] def buildPubsubSchemaClient(): SchemaServiceClient = {
    SchemaServiceClient.create()
  }

  private lazy val delegate: SerDe = buildSerDe(topicInfo)

  private def buildSerDe(topicInfo: TopicInfo): SerDe = {
    val schemaClient = buildPubsubSchemaClient()
    val projectName = topicInfo.params.getOrElse(ProjectKey, throw new IllegalArgumentException(s"$ProjectKey not set"))
    val schemaId = topicInfo.params.getOrElse(SchemaIdKey, throw new IllegalArgumentException(s"$SchemaIdKey not set"))
    val schemaRevisionId = topicInfo.params.get(SchemaRevisionIdKey)
    val schemaName = SchemaName.of(projectName, schemaId)

    val schema =
      try {
        schemaRevisionId match {
          case Some(revisionId) => fetchSchemaRevision(schemaClient, schemaName, revisionId)
          case None             => schemaClient.getSchema(schemaName)
        }
      } catch {
        case e: NotFoundException =>
          val revisionStr = schemaRevisionId.map(rid => s", revisionId: $rid").getOrElse("")
          throw new IllegalArgumentException(
            s"Schema not found - project: $projectName, schemaId: $schemaId$revisionStr",
            e)
        case e: Exception =>
          val revisionStr = schemaRevisionId.map(rid => s", revisionId: $rid").getOrElse("")
          throw new IllegalStateException(
            s"Failed retrieving schema - project: $projectName, schemaId: $schemaId$revisionStr",
            e)
      } finally {
        schemaClient.close()
      }

    schema.getType match {
      case com.google.pubsub.v1.Schema.Type.AVRO =>
        val avroSchema = AvroCodec.of(schema.getDefinition).schema
        new AvroSerDe(avroSchema)
      case com.google.pubsub.v1.Schema.Type.PROTOCOL_BUFFER =>
        val protobufSchema = new ProtobufSchema(schema.getDefinition)
        val descriptor = protobufSchema.toDescriptor()
        new ProtobufSerDe(descriptor, proto3DefaultAsNull)
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported schema type: $other. Supported types are AVRO and PROTOCOL_BUFFER.")
    }
  }

  private def fetchSchemaRevision(schemaClient: SchemaServiceClient,
                                  schemaName: SchemaName,
                                  revisionId: String): com.google.pubsub.v1.Schema = {
    val request = ListSchemaRevisionsRequest
      .newBuilder()
      .setName(schemaName.toString())
      .setView(SchemaView.FULL)
      .setPageSize(2)
      .build();
    val response = schemaClient.listSchemaRevisions(request)
    val revisions = response.iteratePages().iterator().next().getValues().asScala
    revisions.find(_.getRevisionId == revisionId).getOrElse {
      throw new NotFoundException(new IllegalArgumentException(s"Schema revision not found: $revisionId"), null, false)
    }
  }

  override def schema: StructType = delegate.schema

  override def fromBytes(bytes: Array[Byte]): Mutation = delegate.fromBytes(bytes)
}

object PubSubSchemaSerDe {
  val ProjectKey = "project"
  val SchemaIdKey = "schemaId"
  val SchemaRevisionIdKey = "schemaRevisionId"
  val Proto3DefaultAsNullKey = "proto3_default_as_null"
}
