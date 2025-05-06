/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.catalog

import ai.chronon.api.{Constants, PartitionRange, PartitionSpec, Query, QueryUtils}
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import java.io.{PrintWriter, StringWriter}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.{mutable, Seq}
import scala.util.{Failure, Success, Try}

/** Trait to track the table format in use by a Chronon dataset and some utility methods to help
  * retrieve metadata / configure it appropriately at creation time
  */

class TableUtils(@transient val sparkSession: SparkSession) extends Serializable {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val ARCHIVE_TIMESTAMP_FORMAT = "yyyyMMddHHmmss"
  @transient private lazy val archiveTimestampFormatter = DateTimeFormatter
    .ofPattern(ARCHIVE_TIMESTAMP_FORMAT)
    .withZone(ZoneId.systemDefault())
  val partitionColumn: String =
    sparkSession.conf.get("spark.chronon.partition.column", "ds")
  val partitionFormat: String =
    sparkSession.conf.get("spark.chronon.partition.format", "yyyy-MM-dd")
  val partitionSpec: PartitionSpec = PartitionSpec(partitionFormat, WindowUtils.Day.millis)
  val smallModelEnabled: Boolean =
    sparkSession.conf.get("spark.chronon.backfill.small_mode.enabled", "true").toBoolean
  val smallModeNumRowsCutoff: Int =
    sparkSession.conf.get("spark.chronon.backfill.small_mode.cutoff", "5000").toInt
  val backfillValidationEnforced: Boolean =
    sparkSession.conf.get("spark.chronon.backfill.validation.enabled", "true").toBoolean
  // Threshold to control whether to use bloomfilter on join backfill. If the backfill row approximate count is under this threshold, we will use bloomfilter.
  // default threshold is 100K rows
  val bloomFilterThreshold: Long =
    sparkSession.conf.get("spark.chronon.backfill.bloomfilter.threshold", "1000000").toLong
  val checkLeftTimeRange: Boolean =
    sparkSession.conf.get("spark.chronon.join.backfill.check.left_time_range", "false").toBoolean

  private val tableWriteFormat = sparkSession.conf.get("spark.chronon.table_write.format", "").toLowerCase

  // transient because the format provider is not always serializable.
  // for example, BigQueryImpl during reflecting with bq flavor
  @transient private lazy val tableFormatProvider: FormatProvider = FormatProvider.from(sparkSession)

  val joinPartParallelism: Int = sparkSession.conf.get("spark.chronon.join.part.parallelism", "1").toInt
  private val aggregationParallelism: Int = sparkSession.conf.get("spark.chronon.group_by.parallelism", "1000").toInt

  sparkSession.sparkContext.setLogLevel("ERROR")

  def preAggRepartition(df: DataFrame): DataFrame =
    if (df.rdd.getNumPartitions < aggregationParallelism) {
      df.repartition(aggregationParallelism)
    } else {
      df
    }

  def tableReachable(tableName: String): Boolean = {
    Try { sparkSession.catalog.getTable(tableName) } match {
      case Success(_) => true
      case Failure(ex) => {
        logger.info(s"""Couldn't reach $tableName. Error: ${ex.getMessage.red}
             |Call path:
             |${cleanStackTrace(ex).yellow}
             |""".stripMargin)
        false
      }
    }
  }

  def loadTable(tableName: String,
                rangeWheres: Seq[String] = List.empty[String],
                cacheDf: Boolean = false): DataFrame = {
    tableFormatProvider
      .readFormat(tableName)
      .map(_.table(tableName, andPredicates(rangeWheres), cacheDf)(sparkSession))
      .getOrElse(
        throw new RuntimeException(s"Could not load table: ${tableName} with partition filter: ${rangeWheres}"))
  }

  def createDatabase(database: String): Boolean = {
    try {
      val command = s"CREATE DATABASE IF NOT EXISTS $database"
      logger.info(s"Creating database with command: $command")
      sql(command)
      true
    } catch {
      case _: AlreadyExistsException =>
        false // 'already exists' is a swallowable exception
      case e: Exception =>
        logger.error(s"Failed to create database $database", e)
        throw e
    }
  }

  def partitions(tableName: String,
                 subPartitionsFilter: Map[String, String] = Map.empty,
                 partitionRange: Option[PartitionRange] = None,
                 partitionColumnName: String = partitionColumn): List[String] = {
    if (!tableReachable(tableName)) return List.empty[String]
    val rangeWheres = andPredicates(partitionRange.map(whereClauses(_, partitionColumnName)).getOrElse(Seq.empty))

    tableFormatProvider
      .readFormat(tableName)
      .map((format) => {
        logger.info(
          s"Getting partitions for ${tableName} with partitionColumnName ${partitionColumnName} and subpartitions: ${subPartitionsFilter}")
        val partitions =
          format.primaryPartitions(tableName, partitionColumnName, rangeWheres, subPartitionsFilter)(sparkSession)

        if (partitions.isEmpty) {
          logger.info(s"No partitions found for table: $tableName")
        } else {
          logger.info(
            s"Found ${partitions.size}, between (${partitions.min}, ${partitions.max}) partitions for table: $tableName")
        }
        partitions
      })
      .getOrElse(List.empty)

  }

  // Given a table and a query extract the schema of the columns involved as input.
  def getColumnsFromQuery(query: String): Seq[String] = {
    val parser = sparkSession.sessionState.sqlParser
    val logicalPlan = parser.parsePlan(query)
    logicalPlan
      .collect {
        case p: Project =>
          p.projectList.flatMap(p => parser.parseExpression(p.sql).references.map(attr => attr.name))
        case f: Filter => f.condition.references.map(attr => attr.name)
      }
      .flatten
      .map(_.replace("`", ""))
      .distinct
      .sorted
  }

  def getSchemaFromTable(tableName: String): StructType = {
    loadTable(tableName).schema
  }

  def lastAvailablePartition(tableName: String,
                             partitionRange: Option[PartitionRange] = None,
                             subPartitionFilters: Map[String, String] = Map.empty): Option[String] =
    partitions(tableName, subPartitionFilters, partitionRange).reduceOption((x, y) => Ordering[String].max(x, y))

  def firstAvailablePartition(tableName: String,
                              partitionRange: Option[PartitionRange] = None,
                              subPartitionFilters: Map[String, String] = Map.empty): Option[String] =
    partitions(tableName, subPartitionFilters, partitionRange).reduceOption((x, y) => Ordering[String].min(x, y))

  def createTable(df: DataFrame,
                  tableName: String,
                  partitionColumns: List[String] = List.empty,
                  tableProperties: Map[String, String] = null,
                  fileFormat: String): Unit = {

    if (!tableReachable(tableName)) {
      try {
        sql(
          CreationUtils
            .createTableSql(tableName, df.schema, partitionColumns, tableProperties, fileFormat, tableWriteFormat))
      } catch {
        case _: TableAlreadyExistsException =>
          logger.info(s"Table $tableName already exists, skipping creation")
        case e: Exception =>
          logger.error(s"Failed to create table $tableName", e)
          throw e

      }
    }
  }

  def insertPartitions(df: DataFrame,
                       tableName: String,
                       tableProperties: Map[String, String] = null,
                       partitionColumns: List[String] = List(partitionColumn),
                       saveMode: SaveMode = SaveMode.Overwrite,
                       fileFormat: String = "PARQUET",
                       autoExpand: Boolean = false): Unit = {

    // partitions to the last
    val colOrder = df.columns.diff(partitionColumns) ++ partitionColumns

    val dfRearranged = df.select(colOrder.map(colName => df.col(QuotingUtils.quoteIdentifier(colName))): _*)

    createTable(dfRearranged, tableName, partitionColumns, tableProperties, fileFormat)

    if (autoExpand) {
      expandTable(tableName, dfRearranged.schema)
    }

    // Run tableProperties
    Option(tableProperties).filter(_.nonEmpty).foreach { props =>
      sql(CreationUtils.alterTablePropertiesSql(tableName, props))
    }

    val finalizedDf = if (autoExpand) {
      // reselect the columns so that a deprecated columns will be selected as NULL before write
      val tableSchema = getSchemaFromTable(tableName)
      val finalColumns = tableSchema.fieldNames.map(fieldName => {
        val escapedName = QuotingUtils.quoteIdentifier(fieldName)
        if (dfRearranged.schema.fieldNames.contains(fieldName)) {
          df(escapedName)
        } else {
          lit(null).as(escapedName)
        }
      })
      dfRearranged.select(finalColumns: _*)
    } else {
      // if autoExpand is set to false, and an inconsistent df is passed, we want to pass in the df as in
      // so that an exception will be thrown below
      dfRearranged
    }

    TableCache.remove(tableName)

    logger.info(s"Writing to $tableName ...")

    finalizedDf.write
      .mode(saveMode)
      // Requires table to exist before inserting.
      // Fails if schema does not match.
      // Does NOT overwrite the schema.
      // Handles dynamic partition overwrite.
      .insertInto(tableName)

    logger.info(s"Finished writing to $tableName")
  }

  // retains only the invocations from chronon code.
  private def cleanStackTrace(throwable: Throwable): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    throwable.printStackTrace(pw)
    val stackTraceString = sw.toString
    "    " + stackTraceString
      .split("\n")
      .filter(_.contains("chronon"))
      .map(_.replace("at ai.chronon.spark.test.", "").replace("at ai.chronon.spark.", "").stripLeading())
      .mkString("\n    ")
  }

  def sql(query: String): DataFrame = {
    val parallelism = sparkSession.sparkContext.getConf.getInt("spark.default.parallelism", 1000)
    val coalesceFactor = sparkSession.sparkContext.getConf.getInt("spark.chronon.coalesce.factor", 10)
    val stackTraceString = cleanStackTrace(new Throwable())

    logger.info(s"""
         |  ${"---- running query ----".highlight}
         |
         |${("    " + query.trim.replace("\n", "\n    ")).yellow}
         |
         |  ---- call path ----
         |
         |$stackTraceString
         |
         |  ---- end ----
         |""".stripMargin)
    try {
      // Run the query
      val df = sparkSession.sql(query).coalesce(coalesceFactor * parallelism)
      df
    } catch {
      case e: AnalysisException if e.getMessage.contains(" already exists") =>
        logger.warn(s"Non-Fatal: ${e.getMessage}. Query may result in redefinition.")
        sparkSession.sql("SHOW USER FUNCTIONS")
      case e: Exception =>
        logger.error("Error running query:", e)
        throw e
    }
  }

  def chunk(partitions: Set[String]): Seq[PartitionRange] = {
    val sortedDates = partitions.toSeq.sorted
    sortedDates.foldLeft(Seq[PartitionRange]()) { (ranges, nextDate) =>
      if (ranges.isEmpty || partitionSpec.after(ranges.last.end) != nextDate) {
        ranges :+ PartitionRange(nextDate, nextDate)(partitionSpec)
      } else {
        val newRange = PartitionRange(ranges.last.start, nextDate)(partitionSpec)
        ranges.dropRight(1) :+ newRange
      }
    }
  }

  def unfilledRanges(outputTable: String,
                     outputPartitionRange: PartitionRange,
                     inputTables: Option[Seq[String]] = None,
                     inputTableToSubPartitionFiltersMap: Map[String, Map[String, String]] = Map.empty,
                     inputToOutputShift: Int = 0,
                     skipFirstHole: Boolean = true,
                     inputPartitionColumnNames: Seq[String] = Seq(partitionColumn)): Option[Seq[PartitionRange]] = {

    val validPartitionRange = if (outputPartitionRange.start == null) { // determine partition range automatically
      val inputStart = inputTables.flatMap(
        _.map(table =>
          firstAvailablePartition(table,
                                  Option(outputPartitionRange),
                                  inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty))).min)
      assert(
        inputStart.isDefined,
        s"""Either partition range needs to have a valid start or
           |an input table with valid data needs to be present
           |inputTables: $inputTables, partitionRange: $outputPartitionRange
           |""".stripMargin
      )
      outputPartitionRange.copy(start = partitionSpec.shift(inputStart.get, inputToOutputShift))(partitionSpec)
    } else {
      outputPartitionRange
    }
    val outputExisting = partitions(outputTable)
    // To avoid recomputing partitions removed by retention mechanisms we will not fill holes in the very beginning of the range
    // If a user fills a new partition in the newer end of the range, then we will never fill any partitions before that range.
    // We instead log a message saying why we won't fill the earliest hole.
    val cutoffPartition = if (outputExisting.nonEmpty) {
      Seq[String](outputExisting.min, outputPartitionRange.start).filter(_ != null).max
    } else {
      validPartitionRange.start
    }

    val fillablePartitions =
      if (skipFirstHole) {
        validPartitionRange.partitions.toSet.filter(_ >= cutoffPartition)
      } else {
        validPartitionRange.partitions.toSet
      }

    val outputMissing = fillablePartitions -- outputExisting

    val existingInputPartitions =
      for (
        inputTables <- inputTables.toSeq;
        inputPartitionColumnName <- inputPartitionColumnNames;
        table <- inputTables;
        subPartitionFilters = inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty);
        partitionStr <- partitions(table, subPartitionFilters, Option(outputPartitionRange), inputPartitionColumnName)
      ) yield {
        partitionSpec.shift(partitionStr, inputToOutputShift)
      }

    val inputMissing = inputTables
      .map(_ => fillablePartitions -- existingInputPartitions)
      .getOrElse(Set.empty)

    val missingPartitions = outputMissing -- inputMissing
    val missingChunks = chunk(missingPartitions)

    logger.info(s"""
               |Unfilled range computation:
               |   Output table: $outputTable
               |   Missing output partitions: ${outputMissing.toSeq.sorted.prettyInline}
               |   Input tables: ${inputTables.getOrElse(Seq("None")).mkString(", ")}
               |   Missing input partitions: ${inputMissing.toSeq.sorted.prettyInline}
               |   Unfilled Partitions: ${missingPartitions.toSeq.sorted.prettyInline}
               |   Unfilled ranges: ${missingChunks.sorted.mkString("")}
               |""".stripMargin)

    if (missingPartitions.isEmpty) return None
    Some(missingChunks)
  }

  // Needs provider
  def getTableProperties(tableName: String): Option[Map[String, String]] = {
    try {
      val tableId = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
      Some(sparkSession.sessionState.catalog.getTempViewOrPermanentTableMetadata(tableId).properties)
    } catch {
      case _: Exception => None
    }
  }

  // Needs provider
  private def dropTableIfExists(tableName: String): Unit = {
    val command = s"DROP TABLE IF EXISTS $tableName"
    logger.info(s"Dropping table with command: $command")
    sql(command)
  }

  def archiveOrDropTableIfExists(tableName: String, timestamp: Option[Instant]): Unit = {
    val archiveTry = Try(archiveTableIfExists(tableName, timestamp))
    archiveTry.failed.foreach { e =>
      logger.info(s"""Fail to archive table $tableName
           |${e.getMessage}
           |Proceed to dropping the table instead.
           |""".stripMargin)
      dropTableIfExists(tableName)
    }
  }

  // Needs provider
  private def archiveTableIfExists(tableName: String, timestamp: Option[Instant]): Unit = {
    if (tableReachable(tableName)) {
      val humanReadableTimestamp = archiveTimestampFormatter.format(timestamp.getOrElse(Instant.now()))
      val finalArchiveTableName = s"${tableName}_$humanReadableTimestamp"
      val command = s"ALTER TABLE $tableName RENAME TO $finalArchiveTableName"
      logger.info(s"Archiving table with command: $command")
      sql(command)
    }
  }

  /*
   * This method detects new columns that appear in newSchema but not in current table,
   * and append those new columns at the end of the existing table. This allows continuous evolution
   * of a Hive table without dropping or archiving data.
   *
   * Warning: ALTER TABLE behavior also depends on underlying storage solution.
   * To read using Hive, which differentiates Table-level schema and Partition-level schema, it is required to
   * take an extra step to sync Table-level schema into Partition-level schema in order to read updated data
   * in Hive. To read from Spark, this is not required since it always uses the Table-level schema.
   */
  private def expandTable(tableName: String, newSchema: StructType): Unit = {

    val existingSchema = getSchemaFromTable(tableName)
    val existingFieldsMap = existingSchema.fields.map(field => (field.name, field)).toMap

    val inconsistentFields = mutable.ListBuffer[(String, DataType, DataType)]()
    val newFields = mutable.ListBuffer[StructField]()

    newSchema.fields.foreach(field => {
      val fieldName = field.name
      if (existingFieldsMap.contains(fieldName)) {
        val existingDataType = existingFieldsMap(fieldName).dataType

        // compare on catalogString so that we don't check nullability which is not relevant for hive tables
        if (existingDataType.catalogString != field.dataType.catalogString) {
          inconsistentFields += ((fieldName, existingDataType, field.dataType))
        }
      } else {
        newFields += field
      }
    })

    if (inconsistentFields.nonEmpty) {
      throw IncompatibleSchemaException(inconsistentFields)
    }

    val newFieldDefinitions = newFields.map(newField => newField.toDDL)
    val expandTableQueryOpt = if (newFieldDefinitions.nonEmpty) {
      val tableLevelAlterSql =
        s"""ALTER TABLE $tableName
           |ADD COLUMNS (
           |    ${newFieldDefinitions.mkString(",\n    ")}
           |)
           |""".stripMargin

      Some(tableLevelAlterSql)
    } else {
      None
    }

    /* check if any old columns are skipped in new field and send warning */
    val updatedFieldsMap = newSchema.fields.map(field => (field.name, field)).toMap
    val excludedFields = existingFieldsMap.filter { case (name, _) =>
      !updatedFieldsMap.contains(name)
    }.toSeq

    if (excludedFields.nonEmpty) {
      val excludedFieldsStr =
        excludedFields.map(tuple => s"columnName: ${tuple._1} dataType: ${tuple._2.dataType.catalogString}")
      logger.info(
        s"""Warning. Detected columns that exist in Hive table but not in updated schema. These are ignored in DDL.
           |${excludedFieldsStr.mkString("\n")}
           |""".stripMargin)
    }

    if (expandTableQueryOpt.nonEmpty) {
      sql(expandTableQueryOpt.get)

      // set a flag in table props to indicate that this is a dynamic table
      sql(CreationUtils.alterTablePropertiesSql(tableName, Map(Constants.ChrononDynamicTable -> true.toString)))
    }
  }

  private def andPredicates(predicates: Seq[String]): String = {
    val whereStr = predicates.map(p => s"($p)").mkString(" AND ")
    logger.info(s"""Where str: $whereStr""")
    whereStr
  }

  def scanDfBase(selectMap: Map[String, String],
                 table: String,
                 wheres: Seq[String],
                 rangeWheres: Seq[String],
                 fallbackSelects: Option[Map[String, String]] = None,
                 cacheDf: Boolean = false): DataFrame = {

    val selects = QueryUtils.buildSelects(selectMap, fallbackSelects)

    logger.info(s""" Scanning data:
                   |  table: ${table.green}
                   |  selects:
                   |    ${selects.mkString("\n    ").green}
                   |  wheres:
                   |    ${wheres.mkString(",\n    ").green}
                   |  partition filters:
                   |    ${rangeWheres.mkString(",\n    ").green}
                   |""".stripMargin)

    var df = loadTable(table, rangeWheres, cacheDf)

    if (selects.nonEmpty) df = df.selectExpr(selects: _*)

    if (wheres.nonEmpty) {
      val whereStr = andPredicates(wheres)
      df = df.where(whereStr)
    }

    val parallelism = sparkSession.sparkContext.getConf.getInt("spark.default.parallelism", 1000)
    val coalesceFactor = sparkSession.sparkContext.getConf.getInt("spark.chronon.coalesce.factor", 10)

    // TODO: this is a temporary fix to handle the case where the partition column is not a string.
    //  This is the case for partitioned BigQuery native tables.
    (if (df.schema.fieldNames.contains(partitionColumn)) {
       df.withColumn(partitionColumn, date_format(df.col(partitionColumn), partitionFormat))
     } else {
       df
     }).coalesce(coalesceFactor * parallelism)
  }

  def whereClauses(partitionRange: PartitionRange, partitionColumn: String = partitionColumn): Seq[String] = {
    val startClause = Option(partitionRange.start).map(s"$partitionColumn >= '" + _ + "'")
    val endClause = Option(partitionRange.end).map(s"$partitionColumn <= '" + _ + "'")
    (startClause ++ endClause).toSeq
  }

  def scanDf(query: Query,
             table: String,
             fallbackSelects: Option[Map[String, String]] = None,
             range: Option[PartitionRange] = None): DataFrame = {

    val maybeQuery = Option(query)
    val queryPartitionColumn = maybeQuery.flatMap(q => Option(q.partitionColumn)).getOrElse(partitionColumn)
    val rangeWheres = range.map(whereClauses(_, queryPartitionColumn)).getOrElse(Seq.empty)
    val queryWheres = maybeQuery.flatMap(q => Option(q.wheres)).map(_.toScala).getOrElse(Seq.empty)
    val wheres: Seq[String] = rangeWheres ++ queryWheres
    val selects = maybeQuery.flatMap(q => Option(q.selects)).map(_.toScala).getOrElse(Map.empty)

    val scanDf = scanDfBase(selects, table, wheres, rangeWheres, fallbackSelects)

    if (queryPartitionColumn != partitionColumn) {
      scanDf.withColumnRenamed(queryPartitionColumn, partitionColumn)
    } else {
      scanDf
    }
  }
}

object TableUtils {
  def apply(sparkSession: SparkSession) = new TableUtils(sparkSession)
}

sealed case class IncompatibleSchemaException(inconsistencies: Seq[(String, DataType, DataType)]) extends Exception {
  override def getMessage: String = {
    val inconsistenciesStr =
      inconsistencies.map(tuple => s"columnName: ${tuple._1} existingType: ${tuple._2} newType: ${tuple._3}")
    s"""Existing columns cannot be modified:
       |${inconsistenciesStr.mkString("\n")}
       |""".stripMargin
  }
}
