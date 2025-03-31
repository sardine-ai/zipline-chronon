package ai.chronon.flink.test

import ai.chronon.api.{Accuracy, Builders, GroupBy}
import ai.chronon.flink.SourceIdentitySchemaRegistrySchemaProvider
import ai.chronon.flink.SourceIdentitySchemaRegistrySchemaProvider.RegistryHostKey
import io.confluent.kafka.schemaregistry.SchemaProvider
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class MockSchemaRegistrySchemaProvider(conf: Map[String, String], mockSchemaRegistryClient: MockSchemaRegistryClient)
    extends SourceIdentitySchemaRegistrySchemaProvider(conf) {
  override def buildSchemaRegistryClient(schemeString: String,
                                         registryHost: String,
                                         maybePortString: Option[String]): MockSchemaRegistryClient =
    mockSchemaRegistryClient
}

class SchemaRegistrySchemaProviderSpec extends AnyFlatSpec {

  private val avroSchemaProvider: SchemaProvider = new AvroSchemaProvider
  private val protoSchemaProvider: SchemaProvider = new ProtobufSchemaProvider
  val schemaRegistryClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider, protoSchemaProvider).asJava)
  private val schemaRegistrySchemaProvider =
    new MockSchemaRegistrySchemaProvider(Map(RegistryHostKey -> "localhost"), schemaRegistryClient)

  it should "fail if the schema subject is not found" in {
    val topicInfo = "kafka://test-topic"
    val groupBy = makeGroupBy(topicInfo)
    assertThrows[IllegalArgumentException] {
      schemaRegistrySchemaProvider.buildDeserializationSchema(groupBy)
    }
  }

  it should "succeed if we look up an avro schema that is present" in {
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("test-topic-avro-value", new AvroSchema(avroSchemaStr))
    val topicInfo = "kafka://test-topic-avro"
    val groupBy = makeGroupBy(topicInfo)
    val deserSchema = schemaRegistrySchemaProvider.buildDeserializationSchema(groupBy)
    assert(deserSchema != null)
  }

  it should "succeed if we look up an avro schema using injected subject" in {
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("my-subject", new AvroSchema(avroSchemaStr))
    val topicInfo = "kafka://another-topic/subject=my-subject"
    val groupBy = makeGroupBy(topicInfo)
    val deserSchema = schemaRegistrySchemaProvider.buildDeserializationSchema(groupBy)
    assert(deserSchema != null)
  }

  it should "fail if we're trying to retrieve a proto schema" in {
    val protoSchemaStr = "message Foo { required string f" + " = 1; }"
    schemaRegistryClient.register("test-topic-proto-value", new ProtobufSchema(protoSchemaStr))
    val topicInfo = "kafka://test-topic-proto"
    val groupBy = makeGroupBy(topicInfo)
    assertThrows[IllegalArgumentException] {
      schemaRegistrySchemaProvider.buildDeserializationSchema(groupBy)
    }
  }

  def makeGroupBy(topicInfo: String): GroupBy = {
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.my_stream_raw",
          topic = topicInfo,
          query = Builders.Query(
            selects = Map(
              "id" -> "id",
              "int_val" -> "int_val",
              "double_val" -> "double_val"
            ),
            wheres = Seq.empty,
            timeColumn = "created",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = Seq("id"),
      accuracy = Accuracy.TEMPORAL
    )
  }
}
