package ai.chronon.online.serde

import ai.chronon.api.{Constants, StructType}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader

import java.io.{ByteArrayInputStream, InputStream}

abstract class Serde extends Serializable {
  def fromBytes(bytes: Array[Byte]): Mutation
  def schema: StructType
  def toBytes(mutation: Mutation): Array[Byte] = {
    // not implemented
    throw new UnsupportedOperationException("toBytes not implemented")
  }
}

/** ==== MUTATION vs. EVENT ====
  * Mutation is the general case of an Event
  * Imagine a user impression/view stream - impressions/views are immutable events
  * Imagine a stream of changes to a credit card transaction stream.
  *    - transactions can be "corrected"/updated & deleted, besides being "inserted"
  *    - This is one of the core difference between entity and event sources. Events are insert-only.
  *    - (The other difference is Entites are stored in the warehouse typically as snapshots of the table as of midnight)
  *      In case of an update - one must produce both before and after values
  *      In case of a delete - only before is populated & after is left as null
  *      In case of a insert - only after is populated & before is left as null
  *
  *       ==== TIME ASSUMPTIONS ====
  *      The schema needs to contain a `ts`(milliseconds as a java Long)
  *      For the entities case, `mutation_ts` when absent will use `ts` as a replacement
  *
  *       ==== TYPE CONVERSIONS ====
  *      Java types corresponding to the schema types. [[Serde]] should produce mutations that comply.
  *      NOTE: everything is nullable (hence boxed)
  *      IntType        java.lang.Integer
  *      LongType       java.lang.Long
  *      DoubleType     java.lang.Double
  *      FloatType      java.lang.Float
  *      ShortType      java.lang.Short
  *      BooleanType    java.lang.Boolean
  *      ByteType       java.lang.Byte
  *      StringType     java.lang.String
  *      BinaryType     Array[Byte]
  *      ListType       java.util.List[Byte]
  *      MapType        java.util.Map[Byte]
  *      StructType     Array[Any]
  */
case class Mutation(schema: StructType = null, before: Array[Any] = null, after: Array[Any] = null)

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
