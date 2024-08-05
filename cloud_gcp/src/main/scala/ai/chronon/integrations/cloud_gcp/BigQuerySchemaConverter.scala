package ai.chronon.integrations.cloud_gcp

import ai.chronon.api._
import com.google.cloud.bigquery.{Field, Schema, StandardSQLTypeName}

object BigQuerySchemaConverter {
  def convertToBigQuerySchema(dataType: DataType): Schema = {
    Schema.of(convertToBigQueryField("root", dataType))
  }

  private def convertToBigQueryField(name: String, dataType: DataType): Field = {
    dataType match {
      case IntType       => Field.of(name, StandardSQLTypeName.INT64)
      case LongType      => Field.of(name, StandardSQLTypeName.INT64)
      case DoubleType    => Field.of(name, StandardSQLTypeName.FLOAT64)
      case FloatType     => Field.of(name, StandardSQLTypeName.FLOAT64)
      case ShortType     => Field.of(name, StandardSQLTypeName.INT64)
      case BooleanType   => Field.of(name, StandardSQLTypeName.BOOL)
      case ByteType      => Field.of(name, StandardSQLTypeName.BYTES)
      case StringType    => Field.of(name, StandardSQLTypeName.STRING)
      case BinaryType    => Field.of(name, StandardSQLTypeName.BYTES)
      case DateType      => Field.of(name, StandardSQLTypeName.DATE)
      case TimestampType => Field.of(name, StandardSQLTypeName.TIMESTAMP)

      case ListType(elementType) =>
        Field
          .newBuilder(name, StandardSQLTypeName.ARRAY)
          .setMode(Field.Mode.REPEATED)
          .setType(convertToBigQueryField("element", elementType).getType)
          .build()

      case MapType(keyType, valueType) =>
        Field
          .newBuilder(name, StandardSQLTypeName.ARRAY)
          .setMode(Field.Mode.REPEATED)
          .setType(
            StandardSQLTypeName.STRUCT,
            Field.of("key", convertToBigQueryField("key", keyType).getType),
            Field.of("value", convertToBigQueryField("value", valueType).getType)
          )
          .build()

      case StructType(_, fields) =>
        Field
          .newBuilder(name, StandardSQLTypeName.STRUCT)
          .setType(StandardSQLTypeName.STRUCT, fields.map(f => convertToBigQueryField(f.name, f.fieldType)): _*)
          .build()

      case UnknownType(_) =>
        Field.of(name, StandardSQLTypeName.STRING) // Default to STRING for unknown types
    }
  }
}
