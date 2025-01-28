package ai.chronon.spark.format

import ai.chronon.spark.format.CreationUtils.alterTablePropertiesSql
import ai.chronon.spark.format.CreationUtils.createTableSql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory
trait Format {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def name: String

  def options: Map[String, String] = Map.empty[String, String]

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

  // Return a sequence for partitions where each partition entry consists of a map of partition keys to values
  // e.g. Seq(
  //         Map("ds" -> "2023-04-01", "hr" -> "12"),
  //         Map("ds" -> "2023-04-01", "hr" -> "13")
  //         Map("ds" -> "2023-04-02", "hr" -> "00")
  //      )
  def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]]

  def createTable(df: DataFrame,
                  tableName: String,
                  partitionColumns: Seq[String],
                  tableProperties: Map[String, String],
                  fileFormat: String): (String => Unit) => Unit = {

    def inner(df: DataFrame,
              tableName: String,
              partitionColumns: Seq[String],
              tableProperties: Map[String, String],
              fileFormat: String)(sqlEvaluator: String => Unit) = {
      val creationSql =
        createTableSql(tableName,
                       df.schema,
                       partitionColumns,
                       tableProperties,
                       fileFormatString(fileFormat),
                       createTableTypeString)

      sqlEvaluator(creationSql)

    }

    inner(df, tableName, partitionColumns, tableProperties, fileFormat)

  }

  def alterTableProperties(tableName: String, tableProperties: Map[String, String]): (String => Unit) => Unit = {

    def inner(tableName: String, tableProperties: Map[String, String])(sqlEvaluator: String => Unit) = {
      val alterSql =
        alterTablePropertiesSql(tableName, tableProperties)

      sqlEvaluator(alterSql)

    }

    inner(tableName, tableProperties)

  }

  // Help specify the appropriate table type to use in the Spark create table DDL query
  def createTableTypeString: String

  // Help specify the appropriate file format to use in the Spark create table DDL query
  def fileFormatString(format: String): String

  // Does this format support sub partitions filters
  def supportSubPartitionsFilter: Boolean

  // TODO: remove this once all formats implement table creation
  val canCreateTable: Boolean = false

}
