package ai.chronon.spark.format

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Success, Try}

/** Default format provider implementation based on default Chronon supported open source library versions.
  */
class DefaultFormatProvider(val sparkSession: SparkSession) extends FormatProvider {

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
}
