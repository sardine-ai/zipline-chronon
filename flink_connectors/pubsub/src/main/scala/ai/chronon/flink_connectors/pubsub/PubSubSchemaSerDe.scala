package ai.chronon.flink_connectors.pubsub

import ai.chronon.api.StructType
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroCodec, AvroSerDe, Mutation, ProtobufSerDe, RequiresMessageAttribute, SerDe}
import com.google.api.gax.rpc.NotFoundException
import com.google.cloud.pubsub.v1.SchemaServiceClient
import com.google.pubsub.v1.ListSchemaRevisionsRequest;
import com.google.pubsub.v1.SchemaName
import com.google.pubsub.v1.SchemaView;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.apache.avro.{Schema => AvroSchema}
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** SerDe that fetches schemas from GCP Pub/Sub Schema Registry and auto-detects the format (Avro or Protobuf).
  *
  * Configure via topic string:
  *   pubsub://topic-name/serde=pubsub_schema/project=my-project/schemaId=my-schema/[proto3_default_as_null=false]
  *
  * Parameters:
  *   - project: GCP project name (required)
  *   - schemaId: Schema ID in Pub/Sub Schema Registry (required)
  *   - proto3_default_as_null: For protobuf schemas, treat proto3 default values as null (optional, defaults to false)
  *
  * For Avro, each message's own `googclient_schemarevisionid` attribute (surfaced via [[RequiresMessageAttribute]]
  * and encoded into the byte stream by the PubSub connector) is used to resolve that message's writer schema on
  * demand, so producers can evolve the schema freely - readers always decode against the latest schema regardless
  * of which revision a given message was written with. Protobuf messages are self-describing and don't need this.
  */
class PubSubSchemaSerDe(topicInfo: TopicInfo) extends SerDe with RequiresMessageAttribute {
  import PubSubSchemaSerDe._

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  private val proto3DefaultAsNull: Boolean =
    topicInfo.params.getOrElse(Proto3DefaultAsNullKey, "false").toBoolean

  private val projectName: String =
    topicInfo.params.getOrElse(ProjectKey, throw new IllegalArgumentException(s"$ProjectKey not set"))
  private val schemaId: String =
    topicInfo.params.getOrElse(SchemaIdKey, throw new IllegalArgumentException(s"$SchemaIdKey not set"))
  private val schemaName: SchemaName = SchemaName.of(projectName, schemaId)

  protected[flink_connectors] def buildPubsubSchemaClient(): SchemaServiceClient = {
    SchemaServiceClient.create()
  }

  private lazy val delegate: SerDe = buildSerDe()

  // Only Avro needs the revision id attribute to resolve a per-message writer schema - Protobuf is
  // self-describing, so we skip the attribute-encoding overhead entirely for it.
  override def attributeKey: Option[String] = delegate match {
    case _: AvroSerDe => Some(RevisionIdAttributeKey)
    case _            => None
  }

  // Writer schemas for revisions other than the latest, resolved on demand as messages carrying the
  // googclient_schemarevisionid attribute arrive, and cached since a schema-service call per message would be
  // prohibitively expensive.
  @transient private lazy val writerSchemaCache = new ConcurrentHashMap[String, AvroSchema]()

  private def withSchemaClient[A](f: SchemaServiceClient => A): A = {
    val schemaClient = buildPubsubSchemaClient()
    try f(schemaClient)
    finally schemaClient.close()
  }

  private def buildSerDe(): SerDe = {
    val schema =
      try {
        withSchemaClient(_.getSchema(schemaName))
      } catch {
        case e: NotFoundException =>
          throw new IllegalArgumentException(s"Schema not found - project: $projectName, schemaId: $schemaId", e)
        case e: Exception =>
          throw new IllegalStateException(s"Failed retrieving schema - project: $projectName, schemaId: $schemaId", e)
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

  private def fetchSchemaRevision(schemaClient: SchemaServiceClient, revisionId: String): com.google.pubsub.v1.Schema = {
    val request = ListSchemaRevisionsRequest
      .newBuilder()
      .setName(schemaName.toString())
      .setView(SchemaView.FULL)
      .setPageSize(10)
      .build();
    val response = schemaClient.listSchemaRevisions(request)
    val revisions = response.iteratePages().iterator().next().getValues().asScala
    revisions.find(_.getRevisionId == revisionId).getOrElse {
      throw new NotFoundException(new IllegalArgumentException(s"Schema revision not found: $revisionId"), null, false)
    }
  }

  private def writerAvroSchema(revisionId: String): AvroSchema =
    writerSchemaCache.computeIfAbsent(
      revisionId,
      rid => {
        logger.info(s"Resolving Avro writer schema for revision $rid (project: $projectName, schemaId: $schemaId)")
        val schema =
          try {
            withSchemaClient(fetchSchemaRevision(_, rid))
          } catch {
            case e: NotFoundException =>
              throw new IllegalArgumentException(
                s"Schema revision not found - project: $projectName, schemaId: $schemaId, revisionId: $rid",
                e)
            case e: Exception =>
              throw new IllegalStateException(
                s"Failed retrieving schema revision - project: $projectName, schemaId: $schemaId, revisionId: $rid",
                e)
          }
        AvroCodec.of(schema.getDefinition).schema
      }
    )

  override def schema: StructType = delegate.schema

  override def fromBytes(bytes: Array[Byte]): Mutation = delegate match {
    case avroSerDe: AvroSerDe =>
      // attributeKey is Some only for this (Avro) branch, so the connector always prefixes these bytes
      // with the length-framed revision id - see DeserializationSchemaWrapper.
      val attributeLength = ByteBuffer.wrap(bytes).getInt
      if (attributeLength < 0) {
        // no revision id attribute on this message (e.g. older client) - fall back to the latest schema
        avroSerDe.fromBytes(bytes.slice(4, bytes.length))
      } else {
        val revisionId = new String(bytes, 4, attributeLength, StandardCharsets.UTF_8)
        val payload = bytes.slice(4 + attributeLength, bytes.length)
        avroSerDe.fromBytes(payload, writerAvroSchema(revisionId))
      }
    case _ =>
      // Protobuf is self-describing (field tags in every message) and never gets the attribute prefix
      // (attributeKey is None for it) - decode the bytes directly.
      delegate.fromBytes(bytes)
  }
}

object PubSubSchemaSerDe {
  val ProjectKey = "project"
  val SchemaIdKey = "schemaId"
  val Proto3DefaultAsNullKey = "proto3_default_as_null"

  // Set by Pub/Sub's client libraries on every message published against a schema-bound topic.
  val RevisionIdAttributeKey = "googclient_schemarevisionid"
}
