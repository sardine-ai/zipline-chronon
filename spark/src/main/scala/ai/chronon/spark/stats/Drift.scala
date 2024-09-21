package ai.chronon.spark.stats

import ai.chronon.api.DataType
import ai.chronon.api.DataType.isNumeric
import ai.chronon.online.{PartitionRange, SparkConversions}
import ai.chronon.spark.TableUtils
import org.apache.spark.sql.DataFrame

case class TransformSpec(where: Seq[String], select: Map[String, String]) {
  def toSql(table: String, partitionRange: PartitionRange)(implicit tableUtils: TableUtils): String = {
    val selectStr = if(select == null || select.isEmpty) {
      "*"
    } else {
      select.map { case (k, v) => s"$v AS $k" }.mkString(", ")
    }

    val partitionWheres = partitionRange.whereClauses(tableUtils.partitionColumn)
    val allWheres = Option(where).getOrElse(Seq()) ++ partitionWheres
    val whereStr = if (allWheres.nonEmpty) {
      "WHERE " + allWheres.map(w => s"($w)").mkString(" AND ")
    } else {
      ""
    }

    s"SELECT $selectStr FROM $table $whereStr"
  }
}

object SemanticType extends Enumeration {
  type SemanticType = Value
  val Continuous, Nominal, Ordinal, Textual, Sequence, Vector = Value

  // default semantic types for given data types, can be overriden in DriftSpec
  def of(dataType: DataType): SemanticType = {
    if (isNumeric(dataType)) {
      Continuous
    } else {
      Nominal
    }
  }
}


case class SemanticOverride(column: String, semanticType: SemanticType.SemanticType, categoryOrdering: Option[Seq[String]])


case class DriftSpec(transform: Option[TransformSpec],
                     keyColumns: Seq[String],
                     timeColumn: String,
                     sliceColumns: Seq[String],
                     semanticOverrides: Seq[SemanticOverride],
                     tileSizeMinutes: Int = 5)

object Drift {
  def computeBatch(table: String, driftSpec: DriftSpec, partitionRange: PartitionRange)(implicit
      tableUtils: TableUtils): DataFrame = {
    // 3. For each tile, aggregate into KLL sketch tiles
    // 4. Compute drift

    // 1. Scan table & get schema
    val transformSpec = driftSpec.transform.getOrElse(TransformSpec(Seq(), Map()))
    val sql = transformSpec.toSql(table, partitionRange)
    val df = tableUtils.sql(sql)
    val schema = df.schema

    // 2. Identify categorical, continuous etc
    val semanticTypes = schema.fields.map { field =>
      val semanticType = driftSpec.semanticOverrides.find(_.column == field.name)
      val categoryOrdering = semanticType.flatMap(_.categoryOrdering)
      val semantic = semanticType.map(_.semanticType).getOrElse {
        if (isNumeric(SparkConversions.toChrononType(field.name, field.dataType))) {
          SemanticType.Continuous
        } else {
          SemanticType.Nominal
        }
      }
      SemanticOverride(field.name, semantic, categoryOrdering)
    }
    null
  }
}
