package ai.chronon.flink_connectors.pubsub

import ai.chronon.api.{IntType, StringType}
import ai.chronon.online.TopicInfo
import com.google.api.gax.rpc.{NotFoundException, StatusCode}
import com.google.cloud.pubsub.v1.SchemaServiceClient
import com.google.protobuf.DynamicMessage
import com.google.pubsub.v1.{ListSchemaRevisionsRequest, Schema, SchemaName}
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec

class MockPubSubSchemaSerDe(topicInfo: TopicInfo, mockSchemaClient: SchemaServiceClient) extends PubSubSchemaSerDe(topicInfo) {
  override def buildPubsubSchemaClient(): SchemaServiceClient = {
    mockSchemaClient
  }
}

class PubSubSchemaSerDeSpec extends AnyFlatSpec {
  it should "fail if the schema is not found" in {
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val statusCode = mock[StatusCode]
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenThrow(new NotFoundException(new IllegalArgumentException(), statusCode, true))

    val pubSubSchemaSerDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    assertThrows[IllegalArgumentException] {
      pubSubSchemaSerDe.schema
    }
  }

  it should "fail if the schema type is unsupported" in {
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder().setName("test-schema").setType(Schema.Type.TYPE_UNSPECIFIED).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val pubSubSchemaSerDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    assertThrows[IllegalArgumentException] {
      pubSubSchemaSerDe.schema
    }
  }

  it should "succeed if the schema is found and is of type AVRO" in {
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    val schema = Schema.newBuilder().setName("test-schema").setType(Schema.Type.AVRO).setDefinition(avroSchemaStr).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val pubSubSchemaSerDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    val deSerSchema = pubSubSchemaSerDe.schema
    assert(deSerSchema != null)
  }

  // ============== Proto3 Tests ==============

  it should "succeed if the schema is found and is of type PROTOCOL_BUFFER (proto3)" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestProto3 {
        |  string name = 1;
        |  int32 age = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-topic", "pubsub",
      Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("test-schema")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto3SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    val chrononSchema = serDe.schema
    assert(chrononSchema != null)
    assert(chrononSchema.fields.length == 2)
    assert(chrononSchema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(chrononSchema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto3 messages from PubSub" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message User {
        |  string username = 1;
        |  int32 user_id = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-topic", "pubsub",
      Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "user-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("user-schema")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto3SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)

    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("username"), "alice")
      .setField(descriptor.findFieldByName("user_id"), 42)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "alice")
    assert(mutation.after(1) == 42)
  }

  it should "handle proto3DefaultAsNull parameter for proto3 schemas from PubSub" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestDefaults {
        |  string text = 1;
        |  int32 number = 2;
        |}""".stripMargin

    val topicInfoWithNull = TopicInfo("test-topic", "pubsub",
      Map(
        PubSubSchemaSerDe.ProjectKey -> "test-project",
        PubSubSchemaSerDe.SchemaIdKey -> "test-defaults",
        PubSubSchemaSerDe.Proto3DefaultAsNullKey -> "true"
      ))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("test-defaults")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto3SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDeWithNull = new MockPubSubSchemaSerDe(topicInfoWithNull, mockedSchemaClient)

    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    val descriptor = protobufSchema.toDescriptor()
    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutationWithNull = serDeWithNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithNull.after(0) == null)
    assert(mutationWithNull.after(1) == null)

    val topicInfoWithoutNull = TopicInfo("test-topic", "pubsub",
      Map(
        PubSubSchemaSerDe.ProjectKey -> "test-project",
        PubSubSchemaSerDe.SchemaIdKey -> "test-defaults",
        PubSubSchemaSerDe.Proto3DefaultAsNullKey -> "false"
      ))
    val serDeWithoutNull = new MockPubSubSchemaSerDe(topicInfoWithoutNull, mockedSchemaClient)

    val mutationWithoutNull = serDeWithoutNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithoutNull.after(0) == "")
    assert(mutationWithoutNull.after(1) == 0)
  }

  // ============== Avro Schema Evolution Tests ==============

  private def lengthPrefixed(revisionId: String, payload: Array[Byte]): Array[Byte] = {
    val idBytes = revisionId.getBytes("UTF-8")
    val buf = java.nio.ByteBuffer.allocate(4 + idBytes.length + payload.length)
    buf.putInt(idBytes.length)
    buf.put(idBytes)
    buf.put(payload)
    buf.array()
  }

  it should "advertise the revision id attribute key for AVRO schemas" in {
    val avroSchemaStr = """{ "type": "record", "name": "test1", "fields": [ { "type": "string", "name": "field1" } ]}"""
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder().setName("test-schema").setType(Schema.Type.AVRO).setDefinition(avroSchemaStr).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    assert(serDe.attributeKey.contains(PubSubSchemaSerDe.RevisionIdAttributeKey))
  }

  it should "not advertise an attribute key for PROTOCOL_BUFFER schemas" in {
    val proto3SchemaStr = """syntax = "proto3"; message TestProto3 { string name = 1; }"""
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder().setName("test-schema").setType(Schema.Type.PROTOCOL_BUFFER).setDefinition(proto3SchemaStr).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    assert(serDe.attributeKey.isEmpty)
  }

  it should "decode using the latest schema when the message carries no revision id" in {
    val avroSchemaStr = """{ "type": "record", "name": "test1", "fields": [ { "type": "string", "name": "field1" } ]}"""
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder().setName("test-schema").setType(Schema.Type.AVRO).setDefinition(avroSchemaStr).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    val record = new org.apache.avro.generic.GenericData.Record(new org.apache.avro.Schema.Parser().parse(avroSchemaStr))
    record.put("field1", "hello")
    val payload = ai.chronon.online.serde.AvroCodec.of(avroSchemaStr).encodeBinary(record)

    // -1 sentinel: no attribute present on this message
    val framed = java.nio.ByteBuffer.allocate(4 + payload.length).putInt(-1).put(payload).array()

    val mutation = serDe.fromBytes(framed)
    assert(mutation.after != null)
    assert(mutation.after(0) == "hello")
  }

  it should "resolve and cache a message's writer schema from its revision id attribute for AVRO schema evolution" in {
    val latestSchemaStr =
      """{ "type": "record", "name": "test1", "fields": [
        |  { "type": "string", "name": "field1" },
        |  { "type": "string", "name": "field2", "default": "unset" }
        |]}""".stripMargin
    val writerSchemaStr = """{ "type": "record", "name": "test1", "fields": [ { "type": "string", "name": "field1" } ]}"""
    val revisionId = "abc123"

    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val latestSchema = Schema.newBuilder().setName("test-schema").setType(Schema.Type.AVRO).setDefinition(latestSchemaStr).build()
    val writerSchema = Schema.newBuilder().setName("test-schema").setRevisionId(revisionId).setType(Schema.Type.AVRO).setDefinition(writerSchemaStr).build()

    val mockedSchemaClient = org.mockito.Mockito.mock(classOf[SchemaServiceClient], org.mockito.Mockito.RETURNS_DEEP_STUBS)
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(latestSchema)
    when(mockedSchemaClient.listSchemaRevisions(any[ListSchemaRevisionsRequest]())
      .iteratePages().iterator().next().getValues())
      .thenReturn(java.util.Collections.singletonList(writerSchema))

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)

    val writerRecord = new org.apache.avro.generic.GenericData.Record(new org.apache.avro.Schema.Parser().parse(writerSchemaStr))
    writerRecord.put("field1", "hello")
    val payload = ai.chronon.online.serde.AvroCodec.of(writerSchemaStr).encodeBinary(writerRecord)

    val mutation = serDe.fromBytes(lengthPrefixed(revisionId, payload))
    assert(mutation.after != null)
    assert(mutation.after(0) == "hello")
    assert(mutation.after(1) == "unset") // reader default applied for the field the writer didn't have

    // a second message on the same revision should hit the cache, not fetch schema revisions again
    serDe.fromBytes(lengthPrefixed(revisionId, payload))
    org.mockito.Mockito.verify(mockedSchemaClient, org.mockito.Mockito.times(1)).listSchemaRevisions(any[ListSchemaRevisionsRequest]())
  }

  // ============== Proto2 Tests ==============

  it should "succeed if the schema is found and is of type PROTOCOL_BUFFER (proto2)" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message TestProto2 {
        |  required string name = 1;
        |  optional int32 age = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-topic", "pubsub",
      Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema-proto2"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("test-schema-proto2")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto2SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    val chrononSchema = serDe.schema
    assert(chrononSchema != null)
    assert(chrononSchema.fields.length == 2)
    assert(chrononSchema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(chrononSchema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto2 messages with required and optional fields from PubSub" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message Person {
        |  required string name = 1;
        |  optional int32 id = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-topic", "pubsub",
      Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "person-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("person-schema")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto2SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)

    val protobufSchema = new ProtobufSchema(proto2SchemaStr)
    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "bob")
      .setField(descriptor.findFieldByName("id"), 123)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "bob")
    assert(mutation.after(1) == 123)
  }

  it should "handle proto2 unset optional fields as null from PubSub" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message OptionalTest {
        |  required string name = 1;
        |  optional int32 value = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-topic", "pubsub",
      Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "optional-test"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("optional-test")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto2SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)

    val protobufSchema = new ProtobufSchema(proto2SchemaStr)
    val descriptor = protobufSchema.toDescriptor()
    val messageWithOnlyRequired = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "test")
      .build()

    val mutation = serDe.fromBytes(messageWithOnlyRequired.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "test")
    assert(mutation.after(1) == null)
  }
}
