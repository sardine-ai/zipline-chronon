package ai.chronon.flink.test

import ai.chronon.flink.SchemaRegistrySchemaProvider
import ai.chronon.flink.SchemaRegistrySchemaProvider.RegistryHostKey
import ai.chronon.online.TopicInfo
import io.confluent.kafka.schemaregistry.SchemaProvider
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class MockSchemaRegistrySchemaProvider(conf: Map[String, String], mockSchemaRegistryClient: MockSchemaRegistryClient)
    extends SchemaRegistrySchemaProvider(conf) {
  override def buildSchemaRegistryClient(schemeString: String,
                                         registryHost: String,
                                         maybePortString: Option[String]): MockSchemaRegistryClient =
    mockSchemaRegistryClient
}

class SchemaRegistrySchemaProviderSpec extends AnyFlatSpec {

  val avroSchemaProvider: SchemaProvider = new AvroSchemaProvider
  val protoSchemaProvider: SchemaProvider = new ProtobufSchemaProvider
  val schemaRegistryClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider, protoSchemaProvider).asJava)
  val schemaRegistrySchemaProvider =
    new MockSchemaRegistrySchemaProvider(Map(RegistryHostKey -> "localhost"), schemaRegistryClient)

  it should "fail if the schema subject is not found" in {
    val topicInfo = new TopicInfo("test-topic", "kafka", Map.empty)
    assertThrows[IllegalArgumentException] {
      schemaRegistrySchemaProvider.buildEncoderAndDeserSchema(topicInfo)
    }
  }

  it should "succeed if we look up an avro schema that is present" in {
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("test-topic-avro-value", new AvroSchema(avroSchemaStr))
    val topicInfo = new TopicInfo("test-topic-avro", "kafka", Map.empty)
    val (encoder, deserSchema) = schemaRegistrySchemaProvider.buildEncoderAndDeserSchema(topicInfo)
    assert(encoder != null)
    assert(deserSchema != null)
  }

  it should "succeed if we look up an avro schema using injected subject" in {
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("my-subject", new AvroSchema(avroSchemaStr))
    val topicInfo = new TopicInfo("another-topic", "kafka", Map("subject" -> "my-subject"))
    val (encoder, deserSchema) = schemaRegistrySchemaProvider.buildEncoderAndDeserSchema(topicInfo)
    assert(encoder != null)
    assert(deserSchema != null)
  }

  it should "fail if we're trying to retrieve a proto schema" in {
    val protoSchemaStr = "message Foo { required string f" + " = 1; }"
    schemaRegistryClient.register("test-topic-proto-value", new ProtobufSchema(protoSchemaStr))
    val topicInfo = new TopicInfo("test-topic-proto", "kafka", Map.empty)
    assertThrows[IllegalArgumentException] {
      schemaRegistrySchemaProvider.buildEncoderAndDeserSchema(topicInfo)
    }
  }
}
