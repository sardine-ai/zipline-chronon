package ai.chronon.online.serde

import ai.chronon.api.{Constants, StructType}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader

import java.io.{ByteArrayInputStream, InputStream}

class AvroSerDe(avroSchema: Schema) extends SerDe {

  lazy val chrononSchema = AvroConversions.toChrononSchema(avroSchema).asInstanceOf[StructType]

  @transient lazy val avroToRowConverter = AvroConversions.genericRecordToChrononRowConverter(chrononSchema)

  private def byteArrayToAvro(avro: Array[Byte], schema: Schema): GenericRecord = {
    val reader = new SpecificDatumReader[GenericRecord](schema)
    val input: InputStream = new ByteArrayInputStream(avro)
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(input, null)
    reader.read(null, decoder)
  }

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val avroRecord = byteArrayToAvro(bytes, avroSchema)

    val row: Array[Any] = avroToRowConverter(avroRecord)

    val reversalIndex = schema.indexWhere(_.name == Constants.ReversalColumn)
    if (reversalIndex >= 0 && row(reversalIndex).asInstanceOf[Boolean]) {
      Mutation(schema, row, null)
    } else {
      Mutation(schema, null, row)
    }
  }

  override def schema: StructType = chrononSchema
}
