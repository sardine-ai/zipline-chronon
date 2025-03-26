package org.apache.spark.sql.avro

import ai.chronon.flink.test.UserAvroSchema
import org.scalatest.flatspec.AnyFlatSpec

class AvroSourceIdentityDeSerializationSupportSpec extends AnyFlatSpec {
  import AvroObjectCreator._

  it should "deserialize avro data" in {
    val schemaStr = UserAvroSchema.schema.toString(true)
    val groupBy = makeMetadataOnlyGroupBy()
    val deserSchema = new AvroSourceIdentityDeserializationSchema(groupBy, schemaStr, schemaRegistryWireFormat = false)
    deserSchema.open(new DummyInitializationContext)
    val recordBytes = createDummyRecordBytes(schemaStr)
    val row = deserSchema.deserialize(recordBytes)
    // sanity check source event schemas line up between the encoder and actual created Row
    val schema = deserSchema.sourceEventEncoder.schema
    assert(schema.fieldNames sameElements row.schema.fieldNames)
    schema.fieldNames.foreach(name => assert(schema(name).dataType == row.schema(name).dataType))
    // spot check a couple of fields
    assert(row.get(0) == 12345)
    assert(row.getString(1) == "johndoe")
  }

  it should "deserialize avro data with schema id" in {
    val schemaStr = UserAvroSchema.schema.toString(true)
    val groupBy = makeMetadataOnlyGroupBy()
    val deserSchema = new AvroSourceIdentityDeserializationSchema(groupBy, schemaStr, schemaRegistryWireFormat = true)
    deserSchema.open(new DummyInitializationContext)
    val recordBytes = AvroObjectCreator.createDummyRecordBytes(schemaStr)
    val recordBytesWithSchemaId = Array[Byte](0, 0, 0, 0, 123) ++ recordBytes
    val row = deserSchema.deserialize(recordBytesWithSchemaId)
    // sanity check schemas line up between the encoder and actual created Row
    val schema = deserSchema.sourceEventEncoder.schema
    assert(schema.fieldNames sameElements row.schema.fieldNames)
    // spot check the id field
    assert(row.get(0) == 12345)
  }

  it should "skip avro data that can't be deserialized" in {

    val schemaStr = UserAvroSchema.schema.toString(true)
    val groupBy = makeMetadataOnlyGroupBy()
    val deserSchema = new AvroSourceIdentityDeserializationSchema(groupBy, schemaStr, schemaRegistryWireFormat = false)
    deserSchema.open(new DummyInitializationContext)
    val recordBytes = AvroObjectCreator.createDummyRecordBytes(schemaStr)
    // corrupt the record bytes
    recordBytes(0) = 0

    val row = deserSchema.deserialize(recordBytes)
    assert(row == null)
  }
}

