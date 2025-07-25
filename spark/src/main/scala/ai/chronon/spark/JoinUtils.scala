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

import ai.chronon.api
import ai.chronon.api._
import ai.chronon.api.DataModel.EVENTS
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.planner.JoinPlanner
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import com.google.gson.Gson
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{coalesce, col, udf}
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

object JoinUtils {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val set_add: UserDefinedFunction =
    udf((set: Seq[String], item: String) => {
      if (set == null && item == null) {
        null
      } else if (set == null) {
        Seq(item)
      } else if (item == null) {
        set
      } else {
        (set :+ item).distinct
      }
    })
  // if either array or query is null or empty, return false
  // if query has an item that exists in array, return true; otherwise, return false
  val contains_any: UserDefinedFunction =
    udf((array: Seq[String], query: Seq[String]) => {
      if (query == null) {
        None
      } else if (array == null) {
        Some(false)
      } else {
        Some(query.exists(q => array.contains(q)))
      }
    })

  /** *
    * Util methods for join computation
    */

  def leftDf(joinConf: ai.chronon.api.Join,
             range: PartitionRange,
             tableUtils: TableUtils,
             allowEmpty: Boolean = false,
             limit: Option[Int] = None): Option[DataFrame] = {

    val timeProjection = if (joinConf.left.dataModel == EVENTS) {
      Seq(Constants.TimeColumn -> Option(joinConf.left.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }

    implicit val tu: TableUtils = tableUtils
    val effectiveLeftSpec = joinConf.left.query.partitionSpec(tableUtils.partitionSpec)
    val effectiveLeftRange = range.translate(effectiveLeftSpec)

    val partitionColumnOfLeft = effectiveLeftSpec.column

    var df = tableUtils.scanDf(joinConf.left.query,
                               joinConf.left.table,
                               Some((Map(partitionColumnOfLeft -> null) ++ timeProjection).toMap),
                               range = Some(effectiveLeftRange))

    limit.foreach(l => df = df.limit(l))

    val skewFilter = joinConf.skewFilter()
    val result = skewFilter
      .map(sf => {
        logger.info(s"left skew filter: $sf")
        df.filter(sf)
      })
      .getOrElse(df)

    if (!allowEmpty && result.isEmpty) {
      logger.info(s"Left side query below produced 0 rows in range $effectiveLeftRange, and allowEmpty=false.")
      return None
    }

    Some(result.translatePartitionSpec(effectiveLeftSpec, tableUtils.partitionSpec))
  }

  /** *
    * Compute partition range to be filled for given join conf
    */
  def getRangeToFill(leftSource: ai.chronon.api.Source,
                     tableUtils: TableUtils,
                     endPartition: String,
                     overrideStartPartition: Option[String] = None,
                     historicalBackfill: Boolean = true): PartitionRange = {

    val overrideStart = if (historicalBackfill) {
      overrideStartPartition
    } else {
      logger.info(s"Historical backfill is set to false. Backfill latest single partition only: $endPartition")
      Some(endPartition)
    }

    implicit val tu: TableUtils = tableUtils
    val leftSpec = leftSource.query.partitionSpec(tableUtils.partitionSpec)

    val firstAvailablePartitionOpt =
      tableUtils.firstAvailablePartition(leftSource.table,
                                         leftSpec,
                                         subPartitionFilters = leftSource.subPartitionFilters)
    lazy val defaultLeftStart = Option(leftSource.query.startPartition)
      .getOrElse {
        require(
          firstAvailablePartitionOpt.isDefined,
          s"No partitions were found for the join source table: ${leftSource.table}."
        )
        firstAvailablePartitionOpt.get
      }

    val leftStart = overrideStart.getOrElse(defaultLeftStart)
    val leftEnd = Option(leftSource.query.endPartition).getOrElse(endPartition)

    logger.info(s"Attempting to fill join partition range: $leftStart to $leftEnd")
    PartitionRange(leftStart, leftEnd)(leftSpec)
  }

  /** *
    * join left and right dataframes, merging any shared columns if exists by the coalesce rule.
    * fails if there is any data type mismatch between shared columns.
    *
    * The order of output joined dataframe is:
    *   - all keys
    *   - all columns on left (incl. both shared and non-shared) in the original order of left
    *   - all columns on right that are NOT shared by left, in the original order of right
    */
  def coalescedJoin(leftDf: DataFrame, rightDf: DataFrame, keys: Seq[String], joinType: String = "left"): DataFrame = {
    leftDf.validateJoinKeys(rightDf, keys)
    val sharedColumns = rightDf.columns.intersect(leftDf.columns)
    sharedColumns.foreach { column =>
      val leftDataType = leftDf.schema(leftDf.schema.fieldIndex(column)).dataType
      val rightDataType = rightDf.schema(rightDf.schema.fieldIndex(column)).dataType
      assert(leftDataType == rightDataType,
             s"Column '$column' has mismatched data types - left type: $leftDataType vs. right type $rightDataType")
    }

    val joinedDf = leftDf.join(rightDf, keys.toSeq, joinType)
    // find columns that exist both on left and right that are not keys and coalesce them
    val selects = keys.map(col) ++
      leftDf.columns.flatMap { colName =>
        if (keys.contains(colName)) {
          None
        } else if (sharedColumns.contains(colName)) {
          Some(coalesce(leftDf(colName), rightDf(colName)).as(colName))
        } else {
          Some(leftDf(colName))
        }
      } ++
      rightDf.columns.flatMap { colName =>
        if (sharedColumns.contains(colName)) {
          None // already selected previously
        } else {
          Some(rightDf(colName))
        }
      }
    val finalDf = joinedDf.select(selects.toSeq: _*)
    finalDf
  }

  /** *
    * Method to create or replace a view for feature table joining with labels.
    * Label columns will be prefixed with "label" or custom prefix for easy identification
    */
  def createOrReplaceView(viewName: String,
                          leftTable: String,
                          rightTable: String,
                          joinKeys: Array[String],
                          tableUtils: TableUtils,
                          viewProperties: Map[String, String] = null,
                          labelColumnPrefix: String = Constants.LabelColumnPrefix): Unit = {
    val fieldDefinitions = joinKeys.map(field => s"l.`$field`") ++
      tableUtils
        .getSchemaFromTable(leftTable)
        .filterNot(field => joinKeys.contains(field.name))
        .map(field => s"l.`${field.name}`") ++
      tableUtils
        .getSchemaFromTable(rightTable)
        .filterNot(field => joinKeys.contains(field.name))
        .map(field => {
          if (field.name.startsWith(labelColumnPrefix)) {
            s"r.`${field.name}`"
          } else {
            s"r.`${field.name}` AS `${labelColumnPrefix}_${field.name}`"
          }
        })
    val joinKeyDefinitions = joinKeys.map(key => s"l.`$key` = r.`$key`")
    val createFragment = s"""CREATE OR REPLACE VIEW $viewName"""
    val queryFragment =
      s"""
         |  AS SELECT
         |     ${fieldDefinitions.mkString(",\n    ")}
         |    FROM $leftTable AS l LEFT OUTER JOIN $rightTable AS r
         |      ON ${joinKeyDefinitions.mkString(" AND ")}""".stripMargin

    val propertiesFragment = if (viewProperties != null && viewProperties.nonEmpty) {
      s"""    TBLPROPERTIES (
         |    ${viewProperties.toMap.transform((k, v) => s"'$k'='$v'").values.mkString(",\n   ")}
         |    )""".stripMargin
    } else {
      ""
    }
    val sqlStatement = Seq(createFragment, propertiesFragment, queryFragment).mkString("\n")
    tableUtils.sql(sqlStatement)
  }

  /** Generate a Bloom filter for 'joinPart' when the row count to be backfilled falls below a specified threshold.
    * This method anticipates that there will likely be a substantial number of rows on the right side that need to be filtered out.
    * @return bloomfilter map option for right part
    */

  def genBloomFilterIfNeeded(
      joinPart: ai.chronon.api.JoinPart,
      leftDataModel: DataModel,
      unfilledRange: PartitionRange,
      joinLevelBloomMapOpt: Option[util.Map[String, BloomFilter]]): Option[util.Map[String, BloomFilter]] = {

    val rightBlooms = joinLevelBloomMapOpt.map { joinBlooms =>
      joinPart.rightToLeft.iterator
        .map { case (rightCol, leftCol) =>
          rightCol -> joinBlooms.get(leftCol)
        }
        .toMap
        .asJava
    }

    // print bloom sizes
    val bloomSizes = rightBlooms.map { blooms =>
      val sizes = blooms.asScala
        .map { case (rightCol, bloom) =>
          s"$rightCol -> ${bloom.bitSize()}"
        }
      logger.info(s"Bloom sizes: ${sizes.mkString(", ")}")
    }

    logger.info(s"""
           Generating bloom filter for joinPart:
           |  part name : ${joinPart.groupBy.metaData.name},
           |  left type : ${leftDataModel},
           |  right type: ${joinPart.groupBy.dataModel},
           |  accuracy  : ${joinPart.groupBy.inferredAccuracy},
           |  part unfilled range: $unfilledRange,
           |  bloom sizes: $bloomSizes
           |  groupBy: ${joinPart.groupBy.toString}
           |""".stripMargin)
    rightBlooms
  }

  def injectKeyFilter(leftDf: DataFrame, joinPart: api.JoinPart): Unit = {
    // Modifies the joinPart to inject the key filter into the where Clause of GroupBys by hardcoding the keyset
    val groupByKeyNames = joinPart.groupBy.getKeyColumns.asScala

    val collectedLeft = leftDf.collect()

    // clone groupBy before modifying it to prevent concurrent modification
    val groupByClone = joinPart.groupBy.deepCopy()
    joinPart.setGroupBy(groupByClone)

    joinPart.groupBy.sources.asScala.foreach { source =>
      val selectMap = Option(source.rootQuery.getQuerySelects).getOrElse(Map.empty[String, String])
      val groupByKeyExpressions = groupByKeyNames.map { key =>
        key -> selectMap.getOrElse(key, key)
      }.toMap

      groupByKeyExpressions
        .map { case (keyName, groupByKeyExpression) =>
          val leftSideKeyName = joinPart.rightToLeft(keyName)
          logger.info(s"KeyName: $keyName, leftSide KeyName: $leftSideKeyName , " +
            s"Join right to left: ${joinPart.rightToLeft.mkString(", ")}")
          val values = collectedLeft.map(row => row.getAs[Any](leftSideKeyName))
          // Check for null keys, warn if found, err if all null
          val (notNullValues, nullValues) = values.partition(_ != null)
          if (notNullValues.isEmpty) {
            throw new RuntimeException(
              s"No not-null keys found for key: $keyName. Check source table or where clauses.")
          } else if (!nullValues.isEmpty) {
            logger.warn(s"Found ${nullValues.length} null keys for key: $keyName.")
          }

          // Escape single quotes in string values for spark sql
          def escapeSingleQuotes(s: String): String = s.replace("'", "\\'")

          // String manipulate to form valid SQL
          val valueSet = notNullValues.map {
            case s: String => s"'${escapeSingleQuotes(s)}'" // Add single quotes for string values
            case other     => other.toString // Keep other types (like Int) as they are
          }.toSet

          // Form the final WHERE clause for injection
          s"$groupByKeyExpression in (${valueSet.mkString(sep = ",")})"
        }
        .foreach { whereClause =>
          logger.info(s"Injecting where clause: $whereClause into groupBy: ${joinPart.groupBy.metaData.name}")

          val currentWheres = Option(source.rootQuery.getWheres).getOrElse(new util.ArrayList[String]())
          currentWheres.add(whereClause)
          source.rootQuery.setWheres(currentWheres)
        }
    }
  }

  def filterColumns(df: DataFrame, filter: Seq[String]): DataFrame = {
    val columnsToDrop = df.columns
      .filterNot(col => filter.contains(col))
    df.drop(columnsToDrop: _*)
  }

  def tablesToRecompute(joinConf: ai.chronon.api.Join,
                        outputTable: String,
                        tableUtils: TableUtils): collection.Seq[String] = {
    // Finds all join output tables (join parts and final table) that need recomputing (in monolithic spark job mode)
    val gson = new Gson()
    (for (
      props <- tableUtils.getTableProperties(outputTable);
      oldSemanticJson <- props.get(Constants.SemanticHashKey);
      oldSemanticHash = gson.fromJson(oldSemanticJson, classOf[java.util.HashMap[String, String]]).toScala
    ) yield {
      logger.info(s"Comparing Hashes:\nNew: ${joinConf.semanticHash},\nOld: $oldSemanticHash")
      joinConf.tablesToDrop(oldSemanticHash)
    }).getOrElse(collection.Seq.empty)
  }

  def shouldRecomputeLeft(joinConf: ai.chronon.api.Join, outputTable: String, tableUtils: TableUtils): Boolean = {

    if (!tableUtils.tableReachable(outputTable)) return false

    try {

      val gson = new Gson()
      val props = tableUtils.getTableProperties(outputTable)

      val oldSemanticJson = props.get(Constants.SemanticHashKey)
      val oldSemanticHash = gson.fromJson(oldSemanticJson, classOf[java.util.HashMap[String, String]]).toScala

      joinConf.leftChanged(oldSemanticHash)

    } catch {

      case e: Exception =>
        logger.error(s"Error while checking props of table $outputTable. Assuming no semantic change.", e)
        false

    }
  }

  def skewFilter(keys: Option[Seq[String]] = None,
                 skewKeys: Option[Map[String, Seq[String]]],
                 leftKeyCols: Seq[String],
                 joiner: String = " OR "): Option[String] = {
    skewKeys.map { keysMap =>
      val result = keysMap
        .filterKeys(key =>
          keys.forall {
            _.contains(key)
          })
        .map { case (leftKey, values) =>
          assert(
            leftKeyCols.contains(leftKey),
            s"specified skew filter for $leftKey is not used as a key in any join part. " +
              s"Please specify key columns in skew filters: [${leftKeyCols.mkString(", ")}]"
          )
          generateSkewFilterSql(leftKey, values)
        }
        .filter(_.nonEmpty)
        .mkString(joiner)
      logger.info(s"Generated join left side skew filter:\n    $result")
      result
    }
  }

  def partSkewFilter(joinPart: JoinPart,
                     skewKeys: Option[Map[String, Seq[String]]],
                     joiner: String = " OR "): Option[String] = {
    skewKeys.flatMap { keys =>
      val result = keys
        .flatMap { case (leftKey, values) =>
          Option(joinPart.keyMapping)
            .map(_.toScala.getOrElse(leftKey, leftKey))
            .orElse(Some(leftKey))
            .filter(joinPart.groupBy.keyColumns.contains(_))
            .map(generateSkewFilterSql(_, values))
        }
        .filter(_.nonEmpty)
        .mkString(joiner)

      if (result.nonEmpty) {
        logger.info(s"Generated join part skew filter for ${joinPart.groupBy.metaData.name}:\n    $result")
        Some(result)
      } else None
    }
  }

  private def generateSkewFilterSql(key: String, values: Seq[String]): String = {
    val nulls = Seq("null", "Null", "NULL")
    val nonNullFilters = Some(s"$key NOT IN (${values.filterNot(nulls.contains).mkString(", ")})")
    val nullFilters = if (values.exists(nulls.contains)) Some(s"$key IS NOT NULL") else None
    (nonNullFilters ++ nullFilters).mkString(" AND ")
  }

  def runSmallMode(tableUtils: TableUtils, leftDf: DataFrame): Boolean = {
    if (tableUtils.smallModelEnabled) {
      val thresholdCount = leftDf.limit(Some(tableUtils.smallModeNumRowsCutoff + 1).get).count()
      val result = thresholdCount <= tableUtils.smallModeNumRowsCutoff
      if (result) {
        logger.info(s"Counted $thresholdCount rows, running join in small mode.")
      } else {
        logger.info(
          s"Counted greater than ${tableUtils.smallModeNumRowsCutoff} rows, proceeding with normal computation.")
      }
      result
    } else {
      false
    }
  }

  def shiftDays(leftDataModel: DataModel, joinPart: JoinPart, leftRange: PartitionRange): PartitionRange = {
    val shiftDays =
      if (leftDataModel == EVENTS && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
        -1
      } else {
        0
      }

    //  left  | right  | acc
    // events | events | snapshot  => right part tables are not aligned - so scan by leftTimeRange
    // events | events | temporal  => already aligned - so scan by leftRange
    // events | entities | snapshot => right part tables are not aligned - so scan by leftTimeRange
    // events | entities | temporal => right part tables are aligned - so scan by leftRange
    // entities | entities | snapshot => right part tables are aligned - so scan by leftRange
    val rightRange = if (leftDataModel == EVENTS && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
      // Disabling for now
      // val leftTimeRange = leftTimeRangeOpt.getOrElse(leftDf.get.timeRange.toPartitionRange)
      leftRange.shift(shiftDays)
    } else {
      leftRange
    }
    rightRange
  }

  def computeLeftSourceTableName(join: api.Join)(implicit tableUtils: TableUtils): String = {
    new JoinPlanner(join)(tableUtils.partitionSpec).leftSourceNode.metaData.cleanName
  }

  def computeFullLeftSourceTableName(join: api.Join)(implicit tableUtils: TableUtils): String = {
    new JoinPlanner(join)(tableUtils.partitionSpec).leftSourceNode.metaData.outputTable
  }
}
