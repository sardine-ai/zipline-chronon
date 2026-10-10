package ai.chronon.flink_connectors.pubsub.fastack

import ai.chronon.api.StructType
import ai.chronon.flink.deser.ChrononDeserializationSchema
import ai.chronon.online.serde.{Mutation, RequiresMessageAttribute, SerDe}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

class DeserializationSchemaWrapperTest extends AnyFlatSpec with MockitoSugar {

  case class TestData(value: String)

  class TestCollector[T] extends Collector[T] {
    val results: ListBuffer[T] = ListBuffer.empty[T]
    
    override def collect(record: T): Unit = results += record
    override def close(): Unit = {}
  }

  class FakeSerDe extends SerDe {
    override def schema: StructType = null
    override def fromBytes(bytes: Array[Byte]): Mutation = null
  }

  class FakeAttributeSerDe(key: Option[String]) extends FakeSerDe with RequiresMessageAttribute {
    override def attributeKey: Option[String] = key
  }

  private def lengthPrefixed(attributeValue: String, payload: Array[Byte]): Array[Byte] = {
    val attrBytes = attributeValue.getBytes(StandardCharsets.UTF_8)
    val buf = ByteBuffer.allocate(1 + 4 + attrBytes.length + payload.length)
    buf.put(DeserializationSchemaWrapper.MagicByte)
    buf.putInt(attrBytes.length)
    buf.put(attrBytes)
    buf.put(payload)
    buf.array()
  }

  private def sentinelPrefixed(payload: Array[Byte]): Array[Byte] = {
    val buf = ByteBuffer.allocate(1 + 4 + payload.length)
    buf.put(DeserializationSchemaWrapper.MagicByte)
    buf.putInt(-1)
    buf.put(payload)
    buf.array()
  }

  "DeserializationSchemaWrapper" should "delegate open to underlying schema" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val mockContext = mock[DeserializationSchema.InitializationContext]

    wrapper.open(mockContext)

    verify(mockSchema).open(mockContext)
  }

  it should "delegate isEndOfStream to underlying schema" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val testData = TestData("test")

    when(mockSchema.isEndOfStream(testData)).thenReturn(true)

    val result = wrapper.isEndOfStream(testData)

    result shouldBe true
    verify(mockSchema).isEndOfStream(testData)
  }

  it should "delegate getProducedType to underlying schema" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val mockTypeInfo = mock[TypeInformation[TestData]]

    when(mockSchema.getProducedType).thenReturn(mockTypeInfo)

    val result = wrapper.getProducedType

    result shouldBe mockTypeInfo
    verify(mockSchema).getProducedType
  }

  it should "throw UnsupportedOperationException for single deserialize method" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val pubsubMessage = PubsubMessage.newBuilder()
      .setData(ByteString.copyFromUtf8("test"))
      .build()

    an[UnsupportedOperationException] should be thrownBy {
      wrapper.deserialize(pubsubMessage)
    }
  }

  it should "deserialize PubsubMessage data using underlying schema" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val collector = new TestCollector[TestData]()
    val testBytes = "test data".getBytes
    val pubsubMessage = PubsubMessage.newBuilder()
      .setData(ByteString.copyFrom(testBytes))
      .build()

    wrapper.deserialize(pubsubMessage, collector)

    verify(mockSchema).deserialize(testBytes, collector)
  }

  it should "handle null data in PubsubMessage" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val collector = new TestCollector[TestData]()
    val pubsubMessage = PubsubMessage.newBuilder().build()

    wrapper.deserialize(pubsubMessage, collector)

    verify(mockSchema).deserialize(Array.empty[Byte], collector)
  }

  // ============== RequiresMessageAttribute encoding ==============

  it should "prepend the length-framed attribute value when the delegate SerDe requires it and the message carries it" in {
    val mockChrononSchema = mock[ChrononDeserializationSchema[TestData]]
    when(mockChrononSchema.serDe).thenReturn(new FakeAttributeSerDe(Some("revision-id-key")))

    val wrapper = new DeserializationSchemaWrapper[TestData](mockChrononSchema)
    val collector = new TestCollector[TestData]()
    val payload = "payload-bytes".getBytes
    val pubsubMessage = PubsubMessage
      .newBuilder()
      .setData(ByteString.copyFrom(payload))
      .putAttributes("revision-id-key", "abc123")
      .build()

    wrapper.deserialize(pubsubMessage, collector)

    verify(mockChrononSchema).deserialize(lengthPrefixed("abc123", payload), collector)
  }

  it should "prepend a -1 sentinel when the delegate SerDe requires the attribute but the message doesn't carry it" in {
    val mockChrononSchema = mock[ChrononDeserializationSchema[TestData]]
    when(mockChrononSchema.serDe).thenReturn(new FakeAttributeSerDe(Some("revision-id-key")))

    val wrapper = new DeserializationSchemaWrapper[TestData](mockChrononSchema)
    val collector = new TestCollector[TestData]()
    val payload = "payload-bytes".getBytes
    val pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFrom(payload)).build()

    wrapper.deserialize(pubsubMessage, collector)

    verify(mockChrononSchema).deserialize(sentinelPrefixed(payload), collector)
  }

  it should "pass the payload through unchanged when the delegate SerDe's attributeKey is None" in {
    val mockChrononSchema = mock[ChrononDeserializationSchema[TestData]]
    when(mockChrononSchema.serDe).thenReturn(new FakeAttributeSerDe(None))

    val wrapper = new DeserializationSchemaWrapper[TestData](mockChrononSchema)
    val collector = new TestCollector[TestData]()
    val payload = "payload-bytes".getBytes
    val pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFrom(payload)).build()

    wrapper.deserialize(pubsubMessage, collector)

    verify(mockChrononSchema).deserialize(payload, collector)
  }

  it should "pass the payload through unchanged when the delegate SerDe doesn't implement RequiresMessageAttribute" in {
    val mockChrononSchema = mock[ChrononDeserializationSchema[TestData]]
    when(mockChrononSchema.serDe).thenReturn(new FakeSerDe)

    val wrapper = new DeserializationSchemaWrapper[TestData](mockChrononSchema)
    val collector = new TestCollector[TestData]()
    val payload = "payload-bytes".getBytes
    val pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFrom(payload)).build()

    wrapper.deserialize(pubsubMessage, collector)

    verify(mockChrononSchema).deserialize(payload, collector)
  }
}
