package ai.chronon.online.serde

import ai.chronon.api._
import com.google.protobuf.{ByteString, DynamicMessage}
import com.google.protobuf.DescriptorProtos._
import com.google.protobuf.Descriptors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class ProtobufSerDeTest extends AnyFlatSpec with Matchers {

  private def buildSimpleDescriptor(name: String,
                                    fields: Seq[(String, FieldDescriptorProto.Type)]): Descriptors.Descriptor = {
    val fileDescProtoBuilder = FileDescriptorProto
      .newBuilder()
      .setName(s"$name.proto")
      .setPackage("test")
      .setSyntax("proto3")

    val messageBuilder = DescriptorProto.newBuilder().setName(name)
    fields.zipWithIndex.foreach { case ((fieldName, fieldType), idx) =>
      messageBuilder.addField(
        FieldDescriptorProto
          .newBuilder()
          .setName(fieldName)
          .setNumber(idx + 1)
          .setType(fieldType)
          .build()
      )
    }
    fileDescProtoBuilder.addMessageType(messageBuilder.build())

    val fileDescProto = fileDescProtoBuilder.build()
    val fileDesc = Descriptors.FileDescriptor.buildFrom(fileDescProto, Array.empty)
    fileDesc.findMessageTypeByName(name)
  }

  behavior of "ProtobufSerDe"

  it should "create schema from descriptor" in {
    val descriptor = buildSimpleDescriptor("TestMessage",
                                           Seq(
                                             "string_field" -> FieldDescriptorProto.Type.TYPE_STRING,
                                             "int_field" -> FieldDescriptorProto.Type.TYPE_INT32
                                           ))

    val serDe = new ProtobufSerDe(descriptor)

    serDe.schema.name shouldBe "TestMessage"
    serDe.schema.fields.length shouldBe 2
    serDe.schema.fields(0) shouldBe StructField("string_field", StringType)
    serDe.schema.fields(1) shouldBe StructField("int_field", IntType)
  }

  it should "deserialize protobuf bytes to Mutation" in {
    val descriptor = buildSimpleDescriptor("TestMessage",
                                           Seq(
                                             "string_field" -> FieldDescriptorProto.Type.TYPE_STRING,
                                             "int_field" -> FieldDescriptorProto.Type.TYPE_INT32
                                           ))

    val serDe = new ProtobufSerDe(descriptor)

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("string_field"), "hello")
      .setField(descriptor.findFieldByName("int_field"), 42)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)

    mutation.schema shouldBe serDe.schema
    mutation.before shouldBe null
    mutation.after should not be null
    mutation.after(0) shouldBe "hello"
    mutation.after(1) shouldBe 42
  }

  it should "handle empty message" in {
    val descriptor = buildSimpleDescriptor("EmptyMessage", Seq.empty)

    val serDe = new ProtobufSerDe(descriptor)

    serDe.schema.fields shouldBe empty
    serDe.schema.name shouldBe "EmptyMessage"

    val emptyBytes = DynamicMessage.newBuilder(descriptor).build().toByteArray
    val mutation = serDe.fromBytes(emptyBytes)

    mutation.schema shouldBe serDe.schema
    mutation.before shouldBe null
    mutation.after should not be null
    mutation.after.length shouldBe 0
  }

  it should "handle invalid protobuf bytes" in {
    val descriptor = buildSimpleDescriptor("TestMessage",
                                           Seq(
                                             "int_field" -> FieldDescriptorProto.Type.TYPE_INT32
                                           ))

    val serDe = new ProtobufSerDe(descriptor)

    val truncatedVarInt = Array[Byte](0x08, 0x80.toByte)

    intercept[com.google.protobuf.InvalidProtocolBufferException] {
      serDe.fromBytes(truncatedVarInt)
    }
  }

  it should "support proto3DefaultAsNull parameter" in {
    val descriptor = buildSimpleDescriptor("TestMessage",
                                           Seq(
                                             "string_field" -> FieldDescriptorProto.Type.TYPE_STRING,
                                             "int_field" -> FieldDescriptorProto.Type.TYPE_INT32
                                           ))

    val serDeWithNull = new ProtobufSerDe(descriptor, proto3DefaultAsNull = true)
    val serDeWithoutNull = new ProtobufSerDe(descriptor, proto3DefaultAsNull = false)

    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutationWithNull = serDeWithNull.fromBytes(emptyMessage.toByteArray)
    val mutationWithoutNull = serDeWithoutNull.fromBytes(emptyMessage.toByteArray)

    assert(mutationWithNull.after(0) == null)
    assert(mutationWithNull.after(1) == null)

    mutationWithoutNull.after(0) shouldBe ""
    mutationWithoutNull.after(1) shouldBe 0
  }

  it should "handle all primitive types" in {
    val descriptor = buildSimpleDescriptor(
      "Primitives",
      Seq(
        "bool_field" -> FieldDescriptorProto.Type.TYPE_BOOL,
        "int32_field" -> FieldDescriptorProto.Type.TYPE_INT32,
        "int64_field" -> FieldDescriptorProto.Type.TYPE_INT64,
        "float_field" -> FieldDescriptorProto.Type.TYPE_FLOAT,
        "double_field" -> FieldDescriptorProto.Type.TYPE_DOUBLE,
        "string_field" -> FieldDescriptorProto.Type.TYPE_STRING,
        "bytes_field" -> FieldDescriptorProto.Type.TYPE_BYTES
      )
    )

    val serDe = new ProtobufSerDe(descriptor)

    serDe.schema.fields.length shouldBe 7
    serDe.schema.fields(0).fieldType shouldBe BooleanType
    serDe.schema.fields(1).fieldType shouldBe IntType
    serDe.schema.fields(2).fieldType shouldBe LongType
    serDe.schema.fields(3).fieldType shouldBe FloatType
    serDe.schema.fields(4).fieldType shouldBe DoubleType
    serDe.schema.fields(5).fieldType shouldBe StringType
    serDe.schema.fields(6).fieldType shouldBe BinaryType

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("bool_field"), true)
      .setField(descriptor.findFieldByName("int32_field"), 42)
      .setField(descriptor.findFieldByName("int64_field"), 123456789L)
      .setField(descriptor.findFieldByName("float_field"), 3.14f)
      .setField(descriptor.findFieldByName("double_field"), 2.718281828)
      .setField(descriptor.findFieldByName("string_field"), "hello")
      .setField(descriptor.findFieldByName("bytes_field"), ByteString.copyFromUtf8("bytes"))
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)

    mutation.after(0) shouldBe true
    mutation.after(1) shouldBe 42
    mutation.after(2) shouldBe 123456789L
    mutation.after(3) shouldBe 3.14f
    mutation.after(4) shouldBe 2.718281828
    mutation.after(5) shouldBe "hello"
    mutation.after(6) shouldBe "bytes".getBytes
  }
}
