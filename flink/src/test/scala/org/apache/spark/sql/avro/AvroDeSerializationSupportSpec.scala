package org.apache.spark.sql.avro

import ai.chronon.flink.test.UserAvroSchema
import ai.chronon.online.serde.AvroCodec
import org.apache.avro.generic.GenericData
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.util.SimpleUserCodeClassLoader
import org.apache.flink.util.UserCodeClassLoader
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._

class DummyInitializationContext
    extends SerializationSchema.InitializationContext
    with DeserializationSchema.InitializationContext {
  override def getMetricGroup = new UnregisteredMetricsGroup

  override def getUserCodeClassLoader: UserCodeClassLoader =
    SimpleUserCodeClassLoader.create(classOf[DummyInitializationContext].getClassLoader)
}

class AvroDeSerializationSupportSpec extends AnyFlatSpec {

  it should "deserialize avro data" in {
    val schemaStr = UserAvroSchema.schema.toString(true)
    val (encoder, deserSchema) = AvroDeserializationSupport.build("test-topic", schemaStr)
    deserSchema.open(new DummyInitializationContext)
    val recordBytes = AvroObjectCreator.createDummyRecordBytes(schemaStr)
    val row = deserSchema.deserialize(recordBytes)
    // sanity check schemas line up between the encoder and actual created Row
    val schema = encoder.schema
    assert(schema.fieldNames sameElements row.schema.fieldNames)
    schema.fieldNames.foreach(name => assert(schema(name).dataType == row.schema(name).dataType))
    // spot check a couple of fields
    assert(row.get(0) == 12345)
    assert(row.getString(1) == "johndoe")
  }

  it should "deserialize avro data with schema id" in {
    val schemaStr = UserAvroSchema.schema.toString(true)

    val (encoder, deserSchema) =
      AvroDeserializationSupport.build("test-topic", schemaStr, schemaRegistryWireFormat = true)
    deserSchema.open(new DummyInitializationContext)
    val recordBytes = AvroObjectCreator.createDummyRecordBytes(schemaStr)
    val recordBytesWithSchemaId = Array[Byte](0, 0, 0, 0, 123) ++ recordBytes
    val row = deserSchema.deserialize(recordBytesWithSchemaId)
    // sanity check schemas line up between the encoder and actual created Row
    val schema = encoder.schema
    assert(schema.fieldNames sameElements row.schema.fieldNames)
    // spot check the id field
    assert(row.get(0) == 12345)
  }

  it should "skip avro data that can't be deserialized" in {

    val schemaStr = UserAvroSchema.schema.toString(true)
    val (_, deserSchema) = AvroDeserializationSupport.build("test-topic", schemaStr)
    deserSchema.open(new DummyInitializationContext)
    val recordBytes = AvroObjectCreator.createDummyRecordBytes(schemaStr)
    // corrupt the record bytes
    recordBytes(0) = 0

    val row = deserSchema.deserialize(recordBytes)
    assert(row == null)
  }
}

object AvroObjectCreator {
  def createDummyRecordBytes(schemaStr: String): Array[Byte] = {
    // Create the main record
    val avroCodec = AvroCodec.of(schemaStr)
    val schema = avroCodec.schema
    val record = new GenericData.Record(schema)

    // Create the nested address record
    val addressSchema = schema.getField("address").schema()
    val address = new GenericData.Record(addressSchema)
    address.put("street", "123 Main St")
    address.put("city", "San Francisco")
    address.put("country", "USA")
    address.put("postalCode", "94105")

    // Create an array of tags
    val tags = new GenericData.Array[String](
      schema.getField("tags").schema(),
      List("active", "premium", "verified").asJava
    )

    // Create a map of preferences
    val preferences = Map(
      "theme" -> "dark",
      "notifications" -> "enabled",
      "language" -> "en"
    ).asJava

    // Fill in all the fields
    record.put("id", 12345)
    record.put("username", "johndoe")
    record.put("tags", tags)
    record.put("address", address)
    record.put("preferences", preferences)
    record.put("lastLoginTimestamp", System.currentTimeMillis())
    record.put("isActive", true)

    avroCodec.encodeBinary(record)
  }
}
