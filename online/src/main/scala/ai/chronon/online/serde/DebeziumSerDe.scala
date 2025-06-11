package ai.chronon.online.serde

import ai.chronon.api.{Constants, StructType}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader

import java.io.{ByteArrayInputStream, InputStream}

class DebeziumSerDe(avroSchema: Schema, maybeCdcTransport: Option[String] = None) extends AvroSerDe {

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

    cdcTransport match {
      case Some("debezium") => {
        // Extract Debezium fields
        val payload = avroRecord.get(Constants.DebeziumPayloadField).asInstanceOf[GenericRecord]
        val operation = payload.get(Constants.DebeziumOpField).toString

        val before = payload.get(Constants.DebeziumBeforeField).asInstanceOf[GenericRecord]
        val beforeRow: Array[Any] = avroToRowConverter(before)

        val after = payload.get(Constants.DebeziumAfterField).asInstanceOf[GenericRecord]
        val afterRow: Array[Any] = avroToRowConverter(after)

        operation match {
          case "c" => Mutation(chrononSchema, null, afterRow) // Create
          case "u" => Mutation(chrononSchema, beforeRow, afterRow) // Update
          case "d" => Mutation(chrononSchema, beforeRow, null) // Delete
          case _   => throw new IllegalArgumentException(s"Unsupported Debezium operation: $operation")
        }
      }
      case None =>
        val row: Array[Any] = avroToRowConverter(avroRecord)
        val reversalIndex = schema.indexWhere(_.name == Constants.ReversalColumn)
        if (reversalIndex >= 0 && row(reversalIndex).asInstanceOf[Boolean]) {
          Mutation(schema, row, null)
        } else {
          Mutation(schema, null, row)
        }
    }
  }

  override def schema: StructType = chrononSchema

  override def cdcTransport: Option[String] = maybeCdcTransport
}
