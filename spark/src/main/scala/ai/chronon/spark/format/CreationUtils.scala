package ai.chronon.spark.format

import org.apache.spark.sql.types.StructType

object CreationUtils {

  def createTableSql(tableName: String,
                     schema: StructType,
                     partitionColumns: Seq[String],
                     tableProperties: Map[String, String],
                     fileFormatString: String,
                     tableTypeString: String): String = {

    val fieldDefinitions = schema
      .filterNot(field => partitionColumns.contains(field.name))
      .map(field => s"`${field.name}` ${field.dataType.catalogString}")

    val createFragment =
      s"""CREATE TABLE $tableName (
         |    ${fieldDefinitions.mkString(",\n    ")}
         |) $tableTypeString """.stripMargin

    val partitionFragment = if (partitionColumns != null && partitionColumns.nonEmpty) {

      val partitionDefinitions = schema
        .filter(field => partitionColumns.contains(field.name))
        .map(field => s"${field.name} ${field.dataType.catalogString}")

      s"""PARTITIONED BY (
         |    ${partitionDefinitions.mkString(",\n    ")}
         |)""".stripMargin

    } else {
      ""
    }

    val propertiesFragment = if (tableProperties != null && tableProperties.nonEmpty) {
      s"""TBLPROPERTIES (
         |    ${tableProperties.transform((k, v) => s"'$k'='$v'").values.mkString(",\n   ")}
         |)""".stripMargin
    } else {
      ""
    }

    Seq(createFragment, partitionFragment, fileFormatString, propertiesFragment).mkString("\n")

  }

  // Needs provider
  def alterTablePropertiesSql(tableName: String, properties: Map[String, String]): String = {
    // Only SQL api exists for setting TBLPROPERTIES
    val propertiesString = properties
      .map { case (key, value) =>
        s"'$key' = '$value'"
      }
      .mkString(", ")
    s"ALTER TABLE $tableName SET TBLPROPERTIES ($propertiesString)"
  }

}
