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

package ai.chronon.spark

import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Constants, PartitionRange, PartitionSpec, Query, QueryUtils, TsUtils}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.format.CreationUtils.alterTablePropertiesSql
import ai.chronon.spark.format.CreationUtils
import ai.chronon.spark.format.FormatProvider
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.PrintWriter
import java.io.StringWriter
import java.time.format.DateTimeFormatter
import java.time.Instant
import java.time.ZoneId
import scala.collection.Seq
import scala.collection.immutable
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

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

  private val minWriteShuffleParallelism = 200

  // see what's allowed and explanations here: https://sparkbyexamples.com/spark/spark-persistence-storage-levels/
  private val cacheLevelString: String =
    sparkSession.conf.get("spark.chronon.table_write.cache.level", "NONE").toUpperCase()
  private val blockingCacheEviction: Boolean =
    sparkSession.conf.get("spark.chronon.table_write.cache.blocking", "false").toBoolean

  private val tableWriteFormat = sparkSession.conf.get("spark.chronon.table_write.format", "").toLowerCase

  // transient because the format provider is not always serializable.
  // for example, BigQueryImpl during reflecting with bq flavor
  @transient private implicit lazy val tableFormatProvider: FormatProvider = FormatProvider.from(sparkSession)

  private val cacheLevel: Option[StorageLevel] = Try {
    if (cacheLevelString == "NONE") None
    else Some(StorageLevel.fromString(cacheLevelString))
  }.recover { case ex: Throwable =>
    new RuntimeException(s"Failed to create cache level from string: $cacheLevelString", ex).printStackTrace()
    None
  }.get

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

  def loadTable(tableName: String): DataFrame = {
    sparkSession.read.table(tableName)
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

  // return all specified partition columns in a table in format of Map[partitionName, PartitionValue]
  def allPartitions(tableName: String, partitionColumnsFilter: List[String] = List.empty): List[Map[String, String]] = {

    if (!tableReachable(tableName)) return List.empty[Map[String, String]]

    val format = tableFormatProvider
      .readFormat(tableName)
      .getOrElse(
        throw new IllegalStateException(
          s"Could not determine read format of table ${tableName}. It is no longer reachable."))
    val partitionSeq = format.partitions(tableName)(sparkSession)

    if (partitionColumnsFilter.isEmpty) {

      partitionSeq

    } else {

      partitionSeq.map { partitionMap =>
        partitionMap.filterKeys(key => partitionColumnsFilter.contains(key)).toMap
      }

    }
  }

  def partitions(tableName: String,
                 subPartitionsFilter: Map[String, String] = Map.empty,
                 partitionColumnName: String = partitionColumn): List[String] = {

    tableFormatProvider
      .readFormat(tableName)
      .map((format) => {
        val partitions =
          format.primaryPartitions(tableName, partitionColumnName, subPartitionsFilter)(sparkSession)

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

  def lastAvailablePartition(tableName: String, subPartitionFilters: Map[String, String] = Map.empty): Option[String] =
    partitions(tableName, subPartitionFilters).reduceOption((x, y) => Ordering[String].max(x, y))

  def firstAvailablePartition(tableName: String, subPartitionFilters: Map[String, String] = Map.empty): Option[String] =
    partitions(tableName, subPartitionFilters).reduceOption((x, y) => Ordering[String].min(x, y))

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
                       autoExpand: Boolean = false,
                       stats: Option[DfStats] = None,
                       sortByCols: List[String] = List.empty): Unit = {

    // partitions to the last
    val colOrder = df.columns.diff(partitionColumns) ++ partitionColumns

    val dfRearranged: DataFrame = df.select(colOrder.map { case c =>
      df.col(c)
    }: _*)

    createTable(dfRearranged, tableName, partitionColumns, tableProperties, fileFormat)

    if (autoExpand) {
      expandTable(tableName, dfRearranged.schema)
    }

    val finalizedDf = if (autoExpand) {
      // reselect the columns so that a deprecated columns will be selected as NULL before write
      val tableSchema = getSchemaFromTable(tableName)
      val finalColumns = tableSchema.fieldNames.map(fieldName => {
        if (dfRearranged.schema.fieldNames.contains(fieldName)) {
          col(fieldName)
        } else {
          lit(null).as(fieldName)
        }
      })
      dfRearranged.select(finalColumns: _*)
    } else {
      // if autoExpand is set to false, and an inconsistent df is passed, we want to pass in the df as in
      // so that an exception will be thrown below
      dfRearranged
    }

    repartitionAndWrite(finalizedDf, tableName, saveMode, stats, sortByCols)
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

  def columnSizeEstimator(dataType: DataType): Long = {
    dataType match {
      // TODO: improve upon this very basic estimate approach
      case ArrayType(elementType, _)      => 50 * columnSizeEstimator(elementType)
      case StructType(fields)             => fields.map(_.dataType).map(columnSizeEstimator).sum
      case MapType(keyType, valueType, _) => 10 * (columnSizeEstimator(keyType) + columnSizeEstimator(valueType))
      case _                              => 1
    }
  }

  def wrapWithCache[T](opString: String, dataFrame: DataFrame)(func: => T): Try[T] = {
    val start = System.currentTimeMillis()
    cacheLevel.foreach { level =>
      logger.info(s"Starting to cache dataframe before $opString - start @ ${TsUtils.toStr(start)}")
      dataFrame.persist(level)
    }
    def clear(): Unit = {
      cacheLevel.foreach(_ => dataFrame.unpersist(blockingCacheEviction))
      val end = System.currentTimeMillis()
      logger.info(
        s"Cleared the dataframe cache after $opString - start @ ${TsUtils.toStr(start)} end @ ${TsUtils.toStr(end)}")
    }
    Try {
      val t: T = func
      clear()
      t
    }.recoverWith { case ex: Exception =>
      clear()
      Failure(ex)
    }
  }

  private def repartitionAndWrite(df: DataFrame,
                                  tableName: String,
                                  saveMode: SaveMode,
                                  stats: Option[DfStats],
                                  sortByCols: Seq[String] = Seq.empty): Unit = {
    wrapWithCache(s"repartition & write to $tableName", df) {
      logger.info("Repartitioning before writing...")

      val repartitioned =
        if (sparkSession.conf.get("spark.chronon.write.repartition", true.toString).toBoolean)
          repartitionInternal(df, tableName, stats, sortByCols)
        else df

      repartitioned.write
        .mode(saveMode)
        // Requires table to exist before inserting.
        // Fails if schema does not match.
        // Does NOT overwrite the schema.
        // Handles dynamic partition overwrite.
        .option("distribution-mode", "none")
        .option("target-file-size-bytes", (512 * 1024 * 1024).toString)
        .insertInto(tableName)
      logger.info(s"Finished writing to $tableName")
    }.get
  }

  private def repartitionInternal(df: DataFrame,
                                  tableName: String,
                                  stats: Option[DfStats],
                                  sortByCols: Seq[String]): DataFrame = {

    // get row count and table partition count statistics

    val (rowCount: Long, tablePartitionCount: Int) =
      if (df.schema.fieldNames.contains(partitionColumn)) {
        if (stats.isDefined && stats.get.partitionRange.wellDefined) {
          stats.get.count -> stats.get.partitionRange.partitions.length
        } else {
          val result = df.select(count(lit(1)), approx_count_distinct(col(partitionColumn))).head()
          (result.getAs[Long](0), result.getAs[Long](1).toInt)
        }
      } else {
        (df.count(), 1)
      }

    // set to one if tablePartitionCount=0 to avoid division by zero
    val nonZeroTablePartitionCount = if (tablePartitionCount == 0) 1 else tablePartitionCount
    logger.info(s"$rowCount rows requested to be written into table $tableName")
    if (rowCount > 0) {
      val columnSizeEstimate = columnSizeEstimator(df.schema)

      // check if spark is running in local mode or cluster mode
      val isLocal = sparkSession.conf.get("spark.master").startsWith("local")

      // roughly 1 partition count per 1m rows x 100 columns
      val rowCountPerPartition = df.sparkSession.conf
        .getOption(SparkConstants.ChrononRowCountPerPartition)
        .map(_.toDouble)
        .flatMap(value => if (value > 0) Some(value) else None)
        .getOrElse(1e8)

      val totalFileCountEstimate = math.ceil(rowCount * columnSizeEstimate / rowCountPerPartition).toInt
      val dailyFileCountUpperBound = 2000
      val dailyFileCountLowerBound = if (isLocal) 1 else 10
      val dailyFileCountEstimate = totalFileCountEstimate / nonZeroTablePartitionCount + 1
      val dailyFileCountBounded =
        math.max(math.min(dailyFileCountEstimate, dailyFileCountUpperBound), dailyFileCountLowerBound)

      val outputParallelism = df.sparkSession.conf
        .getOption(SparkConstants.ChrononOutputParallelismOverride)
        .map(_.toInt)
        .flatMap(value => if (value > 0) Some(value) else None)

      if (outputParallelism.isDefined) {
        logger.info(s"Using custom outputParallelism ${outputParallelism.get}")
      }
      val dailyFileCount = outputParallelism.getOrElse(dailyFileCountBounded)

      // finalized shuffle parallelism
      val shuffleParallelism = Math.max(dailyFileCount * nonZeroTablePartitionCount, minWriteShuffleParallelism)
      val saltCol = "random_partition_salt"
      val saltedDf = df.withColumn(saltCol, round(rand() * (dailyFileCount + 1)))

      logger.info(
        s"repartitioning data for table $tableName by $shuffleParallelism spark tasks into $tablePartitionCount table partitions and $dailyFileCount files per partition")
      val (repartitionCols: immutable.Seq[String], partitionSortCols: immutable.Seq[String]) =
        if (df.schema.fieldNames.contains(partitionColumn)) {
          (Seq(partitionColumn, saltCol), Seq(partitionColumn) ++ sortByCols)
        } else { (Seq(saltCol), sortByCols) }
      logger.info(s"Sorting within partitions with cols: $partitionSortCols")

      saltedDf
        .repartition(shuffleParallelism, repartitionCols.map(saltedDf.col): _*)
        .drop(saltCol)
        .sortWithinPartitions(partitionSortCols.map(col): _*)
    }
    df
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
                     inputPartitionColumnName: String = partitionColumn): Option[Seq[PartitionRange]] = {

    val validPartitionRange = if (outputPartitionRange.start == null) { // determine partition range automatically
      val inputStart = inputTables.flatMap(_.map(table =>
        firstAvailablePartition(table, inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty))).min)
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
        table <- inputTables;
        subPartitionFilters = inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty);
        partitionStr <- partitions(table, subPartitionFilters, inputPartitionColumnName)
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

    val newFieldDefinitions = newFields.map(newField => s"${newField.name} ${newField.dataType.catalogString}")
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
      sql(alterTablePropertiesSql(tableName, Map(Constants.ChrononDynamicTable -> true.toString)))
    }
  }

  def scanDfBase(selectMap: Map[String, String],
                 table: String,
                 wheres: Seq[String],
                 rangeWheres: Seq[String],
                 fallbackSelects: Option[Map[String, String]] = None): DataFrame = {

    var df = loadTable(table)

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

    if (selects.nonEmpty) df = df.selectExpr(selects: _*)

    val allWheres = wheres ++ rangeWheres
    if (allWheres.nonEmpty) {
      val whereStr = allWheres.map(w => s"($w)").mkString(" AND ")
      logger.info(s"""Where str: $whereStr""")
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

    val queryPartitionColumn = query.effectivePartitionColumn(this)

    val rangeWheres = range.map(whereClauses(_, queryPartitionColumn)).getOrElse(Seq.empty)
    val queryWheres = Option(query).flatMap(q => Option(q.wheres)).map(_.toScala).getOrElse(Seq.empty)
    val wheres: Seq[String] = rangeWheres ++ queryWheres

    val selects = Option(query).flatMap(q => Option(q.selects)).map(_.toScala).getOrElse(Map.empty)

    val scanDf = scanDfBase(selects, table, wheres, rangeWheres, fallbackSelects)

    if (queryPartitionColumn != partitionColumn) {
      scanDf.withColumnRenamed(queryPartitionColumn, partitionColumn)
    } else {
      scanDf
    }
  }

  def partitionRange(table: String): PartitionRange = {
    val parts = partitions(table)
    val minPartition = if (parts.isEmpty) null else parts.min
    val maxPartition = if (parts.isEmpty) null else parts.max
    PartitionRange(minPartition, maxPartition)(partitionSpec)
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
