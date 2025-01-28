package ai.chronon.spark.format

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

case object Iceberg extends Format {

  override def name: String = "iceberg"

  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): Seq[String] = {
    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    getIcebergPartitions(tableName)
  }

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]] = {
    throw new NotImplementedError(
      "Multi-partitions retrieval is not supported on Iceberg tables yet." +
        "For single partition retrieval, please use 'partition' method.")
  }

  private def getIcebergPartitions(tableName: String)(implicit sparkSession: SparkSession): Seq[String] = {

    val partitionsDf = sparkSession.read
      .format("iceberg")
      .load(s"$tableName.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")

    if (partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("hr")) {
      // Hour filter is currently buggy in iceberg. https://github.com/apache/iceberg/issues/4718
      // so we collect and then filter.
      partitionsDf
        .select("partition.ds", "partition.hr")
        .collect()
        .filter(_.get(1) == null)
        .map(_.getString(0))
        .toSeq

    } else {

      partitionsDf
        .select("partition.ds")
        .collect()
        .map(_.getString(0))
        .toSeq
    }
  }

  def createTableTypeString: String = "USING iceberg"

  def fileFormatString(format: String): String = ""

  override def supportSubPartitionsFilter: Boolean = false
}
