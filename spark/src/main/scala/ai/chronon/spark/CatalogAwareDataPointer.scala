package ai.chronon.spark

import ai.chronon.api.DataPointer
import ai.chronon.spark.format.FormatProvider
import org.apache.spark.sql.SparkSession

case class CatalogAwareDataPointer(inputTableOrPath: String, formatProvider: FormatProvider) extends DataPointer {

  override def tableOrPath: String = {
    formatProvider.resolveTableName(inputTableOrPath)
  }

  override lazy val readOptions: Map[String, String] = {
    formatProvider.readFormat(inputTableOrPath).map(_.options).getOrElse(Map.empty)
  }

  override lazy val writeOptions: Map[String, String] = {
    formatProvider.writeFormat(inputTableOrPath).options
  }

  override lazy val readFormat: Option[String] = {
    formatProvider.readFormat(inputTableOrPath).map(_.name)
  }

  override lazy val writeFormat: Option[String] = {
    Option(formatProvider.writeFormat(inputTableOrPath)).map(_.name)
  }

}

object DataPointer {

  def from(tableOrPath: String, sparkSession: SparkSession): DataPointer = {

    CatalogAwareDataPointer(tableOrPath, FormatProvider.from(sparkSession))

  }

}
