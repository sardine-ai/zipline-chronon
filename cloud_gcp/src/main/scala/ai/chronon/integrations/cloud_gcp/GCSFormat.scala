package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.Format
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.url_decode

case class GCS(project: String) extends Format {

  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): Seq[String] =
    super.primaryPartitions(tableName, partitionColumn, subPartitionsFilter)

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]] = {
    import sparkSession.implicits._

    val tableIdentifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    val table = tableIdentifier.table
    val database = tableIdentifier.database.getOrElse(throw new IllegalArgumentException("database required!"))

    // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
    // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
    sparkSession.conf.set("viewsEnabled", "true")
    sparkSession.conf.set("materializationDataset", database)

    // First, grab the URI location from BQ
    val uriSQL =
      s"""
         |select JSON_EXTRACT_STRING_ARRAY(option_value) as option_values from `${project}.${database}.INFORMATION_SCHEMA.TABLE_OPTIONS`
         |WHERE table_name = '${table}' and option_name = 'uris'
         |
         |""".stripMargin

    val uris = sparkSession.read
      .format("bigquery")
      .option("project", project)
      .option("query", uriSQL)
      .load()
      .select(explode(col("option_values")).as("option_value"))
      .select(url_decode(col("option_value")))
      .as[String]
      .collect
      .toList

    assert(uris.length == 1, s"External table ${tableName} can be backed by only one URI.")

    /**
      * Given:
      *  hdfs://<host>:<port>/ path/ to/ partition/ a=1/ b=hello/ c=3.14
      *  hdfs://<host>:<port>/ path/ to/ partition/ a=2/ b=world/ c=6.28
      *
      * it returns:
      * PartitionSpec(
      *  partitionColumns = StructType(
      *    StructField(name = "a", dataType = IntegerType, nullable = true),
      *    StructField(name = "b", dataType = StringType, nullable = true),
      *    StructField(name = "c", dataType = DoubleType, nullable = true)),
      *    partitions = Seq(
      *      Partition(
      *        values = Row(1, "hello", 3.14),
      *        path = "hdfs://<host>:<port>/ path/ to/ partition/ a=1/ b=hello/ c=3.14"),
      *      Partition(
      *        values = Row(2, "world", 6.28),
      *        path = "hdfs://<host>:<port>/ path/ to/ partition/ a=2/ b=world/ c=6.28")))
      *
      */
    val partitionSpec = sparkSession.read
      .parquet(uris: _*)
      .queryExecution
      .sparkPlan
      .asInstanceOf[FileSourceScanExec]
      .relation
      .location
      .asInstanceOf[PartitioningAwareFileIndex] // Punch through the layers!!
      .partitionSpec

    val partitionColumns = partitionSpec.partitionColumns
    val partitions = partitionSpec.partitions.map(_.values)

    partitions
      .map((part) =>
        partitionColumns.fields.toList.zipWithIndex.map {
          case (field, idx) => {
            val fieldName = field.name
            val fieldValue = part.get(idx, field.dataType)
            fieldName -> fieldValue.toString // Just going to cast this as a string.
          }
        }.toMap)
      .toList
  }

  def createTableTypeString: String = throw new UnsupportedOperationException("GCS does not support create table")
  def fileFormatString(format: String): String = ""

  override def supportSubPartitionsFilter: Boolean = true

}
