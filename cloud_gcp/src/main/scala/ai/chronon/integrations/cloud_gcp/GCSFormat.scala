package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.ScalaJavaConversions.JListOps
import ai.chronon.spark.TableUtils
import ai.chronon.spark.TableUtils.{TableCreatedWithInitialData, TableCreationStatus}
import ai.chronon.spark.format.Format
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  ExternalTableDefinition,
  FormatOptions,
  HivePartitioningOptions,
  TableInfo
}
import com.google.cloud.spark.bigquery.{SchemaConverters, SchemaConvertersConfiguration}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.slf4j.LoggerFactory

case class GCS(sourceUri: String, fileFormat: String) extends Format {

  private lazy val logger = LoggerFactory.getLogger(this.getClass.getName)

  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  lazy val bigQueryClient: BigQuery = bqOptions.getService

  override def name: String = fileFormat

  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): Seq[String] =
    super.primaryPartitions(tableName, partitionColumn, subPartitionsFilter)

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]] = {

    /** Given:
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
      */
    val partitionSpec = sparkSession.read
      .format(fileFormat)
      .load(sourceUri)
      .queryExecution
      .sparkPlan
      .asInstanceOf[FileSourceScanExec]
      .relation
      .location
      .asInstanceOf[PartitioningAwareFileIndex] // Punch through the layers!!
      .partitionSpec

    val partitionColumns = partitionSpec.partitionColumns
    val partitions = partitionSpec.partitions.map(_.values)

    val deserializer =
      try {
        Encoders.row(partitionColumns).asInstanceOf[ExpressionEncoder[Row]].resolveAndBind().createDeserializer()
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"Failed to create deserializer for partition columns: ${e.getMessage}", e)
      }

    val roundTripped = sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(partitions.map(deserializer)), partitionColumns)
      .collect

    roundTripped
      .map(part =>
        partitionColumns.fields.iterator.zipWithIndex.map { case (field, idx) =>
          val fieldName = field.name
          val fieldValue = part.get(idx)
          fieldName -> fieldValue.toString // Just going to cast this as a string.

        }.toMap)
  }

  override def generateTableBuilder(df: DataFrame,
                                    tableName: String,
                                    partitionColumns: Seq[String],
                                    tableProperties: Map[String, String],
                                    fileFormat: String): (String => Unit) => TableCreationStatus = {

    def inner(df: DataFrame, tableName: String, partitionColumns: Seq[String])(sqlEvaluator: String => Unit) = {

      // See: https://cloud.google.com/bigquery/docs/partitioned-tables#limitations
      // "BigQuery does not support partitioning by multiple columns. Only one column can be used to partition a table."
      require(partitionColumns.size < 2,
              s"BigQuery only supports at most one partition column, incoming spec: ${partitionColumns}")

      val shadedTableId = BigQueryUtil.parseTableId(tableName)

      val writePrefix = TableUtils(df.sparkSession).writePrefix
      require(writePrefix.nonEmpty, "Please set conf 'spark.chronon.table_write.prefix' pointing to a data bucket.")

      val path = writePrefix.get + tableName.sanitize + "/" //split("/").map(_.sanitize).mkString("/")
      val dataGlob = path + "*"

      logger.info(s"""
           |table source uri: $dataGlob
           |partition uri: $path
           |""".stripMargin)

      df.write
        .partitionBy(partitionColumns: _*)
        .mode("overwrite") // or "append" based on your needs
        .parquet(path)

      val baseTableDef = ExternalTableDefinition
        .newBuilder(dataGlob, FormatOptions.parquet())
        .setAutodetect(true)

      if (partitionColumns.nonEmpty) {
        val timePartitioning = HivePartitioningOptions
          .newBuilder()
          .setFields(partitionColumns.toJava)
          .setSourceUriPrefix(path)
          .setMode("STRINGS")
          .build()
        baseTableDef.setHivePartitioningOptions(timePartitioning)
      }

      val tableInfo = TableInfo.newBuilder(shadedTableId, baseTableDef.build).build()
      val createdTable = bigQueryClient.create(tableInfo)

      println(s"Created external table ${createdTable.getTableId}")

      TableCreatedWithInitialData
    }

    inner(df, tableName, partitionColumns)
  }

  def createTableTypeString: String = throw new UnsupportedOperationException("GCS does not support create table")

  def fileFormatString(format: String): String = ""

  override def supportSubPartitionsFilter: Boolean = true

}
