package ai.chronon.orchestration.test.temporal.converter

import ai.chronon.api.thrift.TBase
import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.temporal.converter.ThriftPayloadConverter
import io.temporal.api.common.v1.Payload
import io.temporal.common.converter.DataConverterException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito.{mock, when}

class ThriftPayloadConverterTest extends AnyFlatSpec with Matchers {

  "ThriftPayloadConverter" should "serialize and deserialize Thrift objects" in {
    val converter = new ThriftPayloadConverter
    val node = new DummyNode().setName("node")

    // Test serialization
    val payload = converter.toData(node)
    payload.isPresent shouldBe true

    // Test deserialization
    val deserializedObject = converter.fromData(payload.get(), classOf[DummyNode], classOf[DummyNode])
    deserializedObject shouldBe a[DummyNode]
    deserializedObject.name shouldBe "node"
  }

  it should "return empty Optional for non-Thrift objects during serialization" in {
    val converter = new ThriftPayloadConverter
    val nonThriftObject = "This is not a Thrift object"

    val payload = converter.toData(nonThriftObject)
    payload.isPresent shouldBe false
  }

  it should "return null for non-Thrift types during deserialization" in {
    val converter = new ThriftPayloadConverter
    val payload = Payload.newBuilder().setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build()

    val result = converter.fromData(payload, classOf[String], classOf[String])
    result shouldBe null
  }

  it should "throw DataConverterException on serialization errors" in {
    val converter = new ThriftPayloadConverter
    val mockTBase = mock(classOf[TBase[_, _]])

    // Mock an exception during serialization
    when(mockTBase.write(org.mockito.ArgumentMatchers.any()))
      .thenThrow(new RuntimeException("Mocked serialization error"))

    an[DataConverterException] should be thrownBy {
      converter.toData(mockTBase)
    }
  }

  it should "throw DataConverterException on deserialization errors" in {
    val converter = new ThriftPayloadConverter
    val invalidPayload =
      Payload.newBuilder().setData(com.google.protobuf.ByteString.copyFromUtf8("invalid data")).build()

    an[DataConverterException] should be thrownBy {
      converter.fromData(invalidPayload, classOf[DummyNode], classOf[DummyNode])
    }
  }

  it should "have the same encoding type as ByteArrayPayloadConverter" in {
    val converter = new ThriftPayloadConverter
    converter.getEncodingType shouldBe "binary/plain"
  }
}
