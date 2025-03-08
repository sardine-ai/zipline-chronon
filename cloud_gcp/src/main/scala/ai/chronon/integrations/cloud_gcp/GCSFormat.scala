package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.format.Format
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.slf4j.LoggerFactory

case class GCS(sourceUri: String, fileFormat: String) extends Format {

  private lazy val logger = LoggerFactory.getLogger(this.getClass.getName)

  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  lazy val bigQueryClient: BigQuery = bqOptions.getService

  override def name: String = fileFormat

  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): List[String] =
    super.primaryPartitions(tableName, partitionColumn, subPartitionsFilter)

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): List[Map[String, String]] = {

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
      .toList
  }

  override def generateTableBuilder(df: DataFrame,
                                    tableName: String,
                                    partitionColumns: List[String],
                                    tableProperties: Map[String, String],
                                    fileFormat: String): (String => Unit) => Unit = {
    throw new UnsupportedOperationException("generateTableBuilder not supported for GCS")
  }

  def createTableTypeString: String = throw new UnsupportedOperationException("GCS does not support create table")

  def fileFormatString(format: String): String = ""

  override def supportSubPartitionsFilter: Boolean = true

}
