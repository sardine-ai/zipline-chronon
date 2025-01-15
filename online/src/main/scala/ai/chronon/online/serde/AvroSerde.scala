package ai.chronon.online.serde

import ai.chronon.api.Constants
import ai.chronon.api.StructType
import ai.chronon.online.AvroConversions
import ai.chronon.online.Mutation
import ai.chronon.online.Serde
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader

import java.io.ByteArrayInputStream
import java.io.InputStream

class AvroSerde(inputSchema: StructType) extends Serde {

  private val avroSchema = AvroConversions.fromChrononSchema(inputSchema)

  private def byteArrayToAvro(avro: Array[Byte], schema: Schema): GenericRecord = {
    val reader = new SpecificDatumReader[GenericRecord](schema)
    val input: InputStream = new ByteArrayInputStream(avro)
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(input, null)
    reader.read(null, decoder)
  }

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val avroRecord = byteArrayToAvro(bytes, avroSchema)

    val row: Array[Any] = schema.fields.map { f =>
      AvroConversions.toChrononRow(avroRecord.get(f.name), f.fieldType).asInstanceOf[AnyRef]
    }

    val reversalIndex = schema.indexWhere(_.name == Constants.ReversalColumn)
    if (reversalIndex >= 0 && row(reversalIndex).asInstanceOf[Boolean]) {
      Mutation(schema, row, null)
    } else {
      Mutation(schema, null, row)
    }

  }

  override def schema: StructType = inputSchema
}
