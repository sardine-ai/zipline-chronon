package org.apache.spark.sql.avro

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

// Utility object to help wrap Spark's AvroDatatoCatalyst classes
object AvroCatalystUtils {

  def buildAvroDataToCatalyst(jsonSchema: String): AvroDataToCatalyst = {
    AvroDataToCatalyst(null, jsonSchema, Map.empty)
  }

  def buildEncoder(jsonSchema: String): Encoder[Row] = {
    val avroDeserializer = buildAvroDataToCatalyst(jsonSchema)
    val catalystType = avroDeserializer.dataType.asInstanceOf[StructType]
    Encoders.row(catalystType)
  }
}
