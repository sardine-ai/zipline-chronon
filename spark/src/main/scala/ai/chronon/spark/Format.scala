package ai.chronon.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.Success
import scala.util.Try

trait Format {

  def name: String

  // Return the primary partitions (based on the 'partitionColumn') filtered down by sub-partition filters if provided
  // If subpartition filters are supplied and the format doesn't support it, we throw an error
  def primaryPartitions(tableName: String,
                        partitionColumn: String,
                        subPartitionsFilter: Map[String, String] = Map.empty)(implicit
      sparkSession: SparkSession): Seq[String] = {
    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    val partitionSeq = partitions(tableName)(sparkSession)
    partitionSeq.flatMap { partitionMap =>
      if (
        subPartitionsFilter.forall {
          case (k, v) => partitionMap.get(k).contains(v)
        }
      ) {
        partitionMap.get(partitionColumn)
      } else {
        None
      }
    }
  }

  // Return a sequence for partitions where each partition entry consists of a Map of partition keys to values
  def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]]

  // Help specify the appropriate table type to use in the Spark create table DDL query
  def createTableTypeString: String

  // Help specify the appropriate file format to use in the Spark create table DDL query
  def fileFormatString(format: String): String

  // Does this format support sub partitions filters
  def supportSubPartitionsFilter: Boolean

}

/**
  * Dynamically provide the read / write table format depending on table name.
  * This supports reading/writing tables with heterogeneous formats.
  * This approach enables users to override and specify a custom format provider if needed. This is useful in
  * cases such as leveraging different library versions from what we support in the Chronon project (e.g. newer delta lake)
  * as well as working with custom internal company logic / checks.
  */
trait FormatProvider extends Serializable {
  def sparkSession: SparkSession
  def readFormat(tableName: String): Format
  def writeFormat(tableName: String): Format

  def resolveTableName(tableName: String) = tableName
}

/**
  * Default format provider implementation based on default Chronon supported open source library versions.
  */
case class DefaultFormatProvider(sparkSession: SparkSession) extends FormatProvider {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Checks the format of a given table by checking the format it's written out as
  override def readFormat(tableName: String): Format = {
    if (isIcebergTable(tableName)) {
      Iceberg
    } else if (isDeltaTable(tableName)) {
      DeltaLake
    } else {
      Hive
    }
  }

  private def isIcebergTable(tableName: String): Boolean =
    Try {
      sparkSession.read.format("iceberg").load(tableName)
    } match {
      case Success(_) =>
        logger.info(s"IcebergCheck: Detected iceberg formatted table $tableName.")
        true
      case _ =>
        logger.info(s"IcebergCheck: Checked table $tableName is not iceberg format.")
        false
    }

  private def isDeltaTable(tableName: String): Boolean = {
    Try {
      val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      describeResult.select("format").first().getString(0).toLowerCase
    } match {
      case Success(format) =>
        logger.info(s"Delta check: Successfully read the format of table: $tableName as $format")
        format == "delta"
      case _ =>
        // the describe detail calls fails for Delta Lake tables
        logger.info(s"Delta check: Unable to read the format of the table $tableName using DESCRIBE DETAIL")
        false
    }
  }

  // Return the write format to use for the given table. The logic at a high level is:
  // 1) If the user specifies the spark.chronon.table_write.iceberg - we go with Iceberg
  // 2) If the user specifies a spark.chronon.table_write.format as Hive (parquet), Iceberg or Delta we go with their choice
  // 3) Default to Hive (parquet)
  // Note the table_write.iceberg is supported for legacy reasons. Specifying "iceberg" in spark.chronon.table_write.format
  // is preferred as the latter conf also allows us to support additional formats
  override def writeFormat(tableName: String): Format = {
    val useIceberg: Boolean = sparkSession.conf.get("spark.chronon.table_write.iceberg", "false").toBoolean

    // Default provider just looks for any default config.
    // Unlike read table, these write tables might not already exist.
    val maybeFormat = sparkSession.conf.getOption("spark.chronon.table_write.format").map(_.toLowerCase) match {
      case Some("hive")    => Some(Hive)
      case Some("iceberg") => Some(Iceberg)
      case Some("delta")   => Some(DeltaLake)
      case _               => None
    }
    (useIceberg, maybeFormat) match {
      // if explicitly configured Iceberg - we go with that setting
      case (true, _) => Iceberg
      // else if there is a write format we pick that
      case (false, Some(format)) => format
      // fallback to hive (parquet)
      case (false, None) => Hive
    }
  }
}

case object Hive extends Format {

  override def name: String = "hive"
  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): Seq[String] =
    super.primaryPartitions(tableName, partitionColumn, subPartitionsFilter)

  def parseHivePartition(pstring: String): Map[String, String] = {
    pstring
      .split("/")
      .map { part =>
        val p = part.split("=", 2)
        p(0) -> p(1)
      }
      .toMap
  }

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]] = {
    // data is structured as a Df with single composite partition key column. Every row is a partition with the
    // column values filled out as a formatted key=value pair
    // Eg. df schema = (partitions: String)
    // rows = [ "day=2020-10-10/hour=00", ... ]
    sparkSession.sqlContext
      .sql(s"SHOW PARTITIONS $tableName")
      .collect()
      .map(row => parseHivePartition(row.getString(0)))
  }

  def createTableTypeString: String = ""

  def fileFormatString(format: String): String = s"STORED AS $format"

  override def supportSubPartitionsFilter: Boolean = true
}

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
    import org.apache.spark.sql.types.StructType
    val partitionsDf = sparkSession.read.format("iceberg").load(s"$tableName.partitions")
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

// The Delta Lake format is compatible with the Delta lake and Spark versions currently supported by the project.
// Attempting to use newer Delta lake library versions (e.g. 3.2 which works with Spark 3.5) results in errors:
// java.lang.NoSuchMethodError: 'org.apache.spark.sql.delta.Snapshot org.apache.spark.sql.delta.DeltaLog.update(boolean)'
// In such cases, you should implement your own FormatProvider built on the newer Delta lake version
case object DeltaLake extends Format {

  override def name: String = "delta"

  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): Seq[String] =
    super.primaryPartitions(tableName, partitionColumn, subPartitionsFilter)

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]] = {
    // delta lake doesn't support the `SHOW PARTITIONS <tableName>` syntax - https://github.com/delta-io/delta/issues/996
    // there's alternative ways to retrieve partitions using the DeltaLog abstraction which is what we have to lean into
    // below
    // first pull table location as that is what we need to pass to the delta log
    val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
    val tablePath = describeResult.select("location").head().getString(0)

    val snapshot = DeltaLog.forTable(sparkSession, tablePath).update()
    val snapshotPartitionsDf = snapshot.allFiles.toDF().select("partitionValues")
    val partitions = snapshotPartitionsDf.collect().map(r => r.getAs[Map[String, String]](0))
    partitions
  }

  def createTableTypeString: String = "USING DELTA"
  def fileFormatString(format: String): String = ""

  override def supportSubPartitionsFilter: Boolean = true
}
