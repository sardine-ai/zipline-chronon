package ai.chronon.spark.format

import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.Success
import scala.util.Try

/**
  * Default format provider implementation based on default Chronon supported open source library versions.
  */
case class DefaultFormatProvider(sparkSession: SparkSession) extends FormatProvider {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Checks the format of a given table if it exists.
  override def readFormat(tableName: String): Option[Format] = {
    Option(if (isIcebergTable(tableName)) {
      Iceberg
    } else if (isDeltaTable(tableName)) {
      DeltaLake
    } else if (sparkSession.catalog.tableExists(tableName)) {
      Hive
    } else { null })
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
