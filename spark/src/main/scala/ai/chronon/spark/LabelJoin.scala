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
import ai.chronon.api.{Builders, Constants, JoinPart, PartitionSpec, TimeUnit, Window}
import ai.chronon.api.DataModel.ENTITIES
import ai.chronon.api.DataModel.EVENTS
import ai.chronon.api.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.api.PartitionRange
import ai.chronon.online.metrics.Metrics
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._
import scala.collection.Seq

class LabelJoin(joinConf: api.Join, tableUtils: TableUtils, labelDS: String) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
  assert(Option(joinConf.metaData.outputNamespace).nonEmpty, "output namespace could not be empty or null")
  assert(
    joinConf.labelParts.leftStartOffset >= joinConf.labelParts.getLeftEndOffset,
    s"Start time offset ${joinConf.labelParts.leftStartOffset} must be earlier than end offset " +
      s"${joinConf.labelParts.leftEndOffset}"
  )

  val metrics: Metrics.Context = Metrics.Context(Metrics.Environment.LabelJoin, joinConf)
  private val outputLabelTable = joinConf.metaData.outputLabelTable
  private val labelJoinConf = joinConf.labelParts
  private val confTableProps = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])

  // offsets are inclusive, e.g label_ds = 04-03, left_start_offset = left_end_offset = 3, left_ds will be 04-01
  val leftStart: String =
    tableUtils.partitionSpec.minus(labelDS, new Window(labelJoinConf.leftStartOffset - 1, TimeUnit.DAYS))
  val leftEnd: String =
    tableUtils.partitionSpec.minus(labelDS, new Window(labelJoinConf.leftEndOffset - 1, TimeUnit.DAYS))

  def computeLabelJoin(stepDays: Option[Int] = None, skipFinalJoin: Boolean = false): DataFrame = {
    // validations
    assert(Option(joinConf.left.dataModel).equals(Option(EVENTS)),
           s"join.left.dataMode needs to be Events for label join ${joinConf.metaData.name}")

    assert(Option(joinConf.metaData.team).nonEmpty,
           s"join.metaData.team needs to be set for join ${joinConf.metaData.name}")

    labelJoinConf.labels.asScala.foreach { jp =>
      if (Option(jp.groupBy.aggregations).isDefined) {
        assert(Option(jp.groupBy.dataModel).equals(Option(EVENTS)),
               s"groupBy.dataModel must be Events for label join with aggregations ${jp.groupBy.metaData.name}")

        assert(Option(jp.groupBy.aggregations).get.size() == 1,
               s"Multiple aggregations not yet supported for label join ${jp.groupBy.metaData.name}")

        assert(
          Option(jp.groupBy.aggregations.get(0).windows).get.size() == 1,
          s"Multiple aggregation windows not yet supported for label join ${jp.groupBy.metaData.name}"
        )

        val aggWindow = jp.groupBy.aggregations.get(0).windows.get(0)
        assert(aggWindow.timeUnit == TimeUnit.DAYS,
               s"${aggWindow.timeUnit} window time unit not supported for label aggregations.")
      } else {
        assert(
          Option(jp.groupBy.dataModel).equals(Option(ENTITIES)),
          s"To perform a none-aggregation label join, the groupBy.dataModel must be entities: ${jp.groupBy.metaData.name}"
        )
      }

      assert(Option(jp.groupBy.metaData.team).nonEmpty,
             s"groupBy.metaData.team needs to be set for label join ${jp.groupBy.metaData.name}")
    }

    Option(labelJoinConf.setups).foreach(_.foreach(tableUtils.sql))
    val labelDf = compute(PartitionRange(leftStart, leftEnd), stepDays, Option(labelDS))

    if (skipFinalJoin) {
      labelDf
    } else {
      // creating final join view with feature join output table
      logger.info(
        s"Joining label table : ${outputLabelTable} with joined output table : ${joinConf.metaData.outputTable}")
      JoinUtils.createOrReplaceView(
        joinConf.metaData.outputFinalView,
        leftTable = joinConf.metaData.outputTable,
        rightTable = outputLabelTable,
        joinKeys = labelJoinConf.rowIdentifier(joinConf.rowIds, tableUtils.partitionColumn),
        tableUtils = tableUtils,
        viewProperties = Map(Constants.LabelViewPropertyKeyLabelTable -> outputLabelTable,
                             Constants.LabelViewPropertyFeatureTable -> joinConf.metaData.outputTable)
      )
      labelDf
    }
  }

  def compute(leftRange: PartitionRange, stepDays: Option[Int] = None, labelDS: Option[String] = None): DataFrame = {
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val sanitizedLabelDs = labelDS.getOrElse(today)
    logger.info(s"Label join range to fill $leftRange")
    def finalResult = tableUtils.scanDf(null, outputLabelTable, range = Some(leftRange))

    val leftFeatureRange = leftRange
    stepDays.foreach(metrics.gauge("step_days", _))
    val stepRanges = stepDays.map(leftFeatureRange.steps).getOrElse(Seq(leftFeatureRange))
    logger.info(s"Label Join left ranges to compute: ${stepRanges.map { _.toString }.pretty}")
    stepRanges.zipWithIndex.foreach { case (range, index) =>
      val startMillis = System.currentTimeMillis()
      val progress = s"| [${index + 1}/${stepRanges.size}]"
      logger.info(s"Computing label join for range: $range  Label DS: ${labelDS.getOrElse(today)} $progress")
      JoinUtils.leftDf(joinConf, range, tableUtils).map { leftDfInRange =>
        computeRange(leftDfInRange, range, sanitizedLabelDs)
          .save(outputLabelTable, confTableProps, Seq(Constants.LabelPartitionColumn, tableUtils.partitionColumn), true)
        val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)
        metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
        metrics.gauge(Metrics.Name.PartitionCount, range.partitions.length)
        logger.info(s"Wrote to table $outputLabelTable, into partitions: $range $progress in $elapsedMins mins")
      }
    }
    logger.info(s"Wrote to table $outputLabelTable, into partitions: $leftFeatureRange")
    finalResult
  }

  def computeRange(leftDf: DataFrame, leftRange: PartitionRange, sanitizedLabelDs: String): DataFrame = {
    val leftDfCount = leftDf.count()
    val leftBlooms = labelJoinConf.leftKeyCols.iterator
      .map { key =>
        key -> leftDf.generateBloomFilter(key, leftDfCount, joinConf.left.table, leftRange)
      }
      .toMap
      .asJava

    // compute joinParts in parallel
    val rightDfs = labelJoinConf.labels.asScala.map { labelJoinPart =>
      val labelJoinPartMetrics = Metrics.Context(metrics, labelJoinPart)
      if (labelJoinPart.groupBy.aggregations == null) {
        // no need to generate join part cache if there are no aggregations
        computelabelParts(labelJoinPart, leftRange, leftBlooms)
      } else {
        val labelOutputRange = PartitionRange(sanitizedLabelDs, sanitizedLabelDs)
        val partTable = joinConf.partOutputTable(labelJoinPart)
        try {
          val leftRanges = tableUtils
            .unfilledRanges(partTable, labelOutputRange, skipFirstHole = false)
            .getOrElse(Seq())
          val partitionCount = leftRanges.map(_.partitions.length).sum
          if (partitionCount > 0) {
            val start = System.currentTimeMillis()
            leftRanges
              .foreach(leftRange => {
                val labeledDf = computelabelParts(labelJoinPart, leftRange, leftBlooms)
                // Cache label part data into intermediate table
                logger.info(s"Writing to join part table: $partTable for partition range $leftRange")
                labeledDf.save(tableName = partTable,
                               tableProperties = confTableProps,
                               partitionColumns = Seq(Constants.LabelPartitionColumn))
              })
            val elapsedMins = (System.currentTimeMillis() - start) / 60000
            labelJoinPartMetrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
            labelJoinPartMetrics.gauge(Metrics.Name.PartitionCount, partitionCount)
            logger.info(s"Wrote ${partitionCount} partitions to label part table: $partTable in $elapsedMins minutes")
          }
        } catch {
          case e: Exception =>
            logger.info(
              "Error while processing groupBy: " +
                s"${joinConf.metaData.name}/${labelJoinPart.groupBy.getMetaData.getName}")
            throw e
        }
        // We need to drop the partition column on the scanned DF because label join doesn't expect a second `ds`
        // On the right side, which will result in a duplicated column error (scan df renames non-default partition cols)
        tableUtils
          .scanDf(query = Builders.Query(partitionColumn = Constants.LabelPartitionColumn),
                  partTable,
                  range = Some(labelOutputRange))
          .drop(tableUtils.partitionColumn)
      }
    }

    val rowIdentifier = labelJoinConf.rowIdentifier(joinConf.rowIds, tableUtils.partitionColumn)
    logger.info("Label Join filtering left df with only row identifier:" + rowIdentifier.mkString(", "))
    val leftFiltered = JoinUtils.filterColumns(leftDf, rowIdentifier)

    val joined = rightDfs.zip(labelJoinConf.labels.asScala).foldLeft(leftFiltered) {
      case (partialDf, (rightDf, joinPart)) => joinWithLeft(partialDf, rightDf, joinPart)
    }

    // assign label ds value and drop duplicates
    val updatedJoin = joined
      .withColumn(Constants.LabelPartitionColumn, lit(sanitizedLabelDs))
      .dropDuplicates(rowIdentifier)
    updatedJoin.explain()
    updatedJoin.drop(Constants.TimePartitionColumn)
  }

  private def computelabelParts(joinPart: JoinPart,
                                leftRange: PartitionRange,
                                leftBlooms: util.Map[String, BloomFilter]): DataFrame = {
    val rightSkewFilter = joinConf.partSkewFilter(joinPart)
    val rightBloomMap = joinPart.rightToLeft.iterator.map { case (right, left) => right -> leftBlooms.get(left) }.toSeq
    val bloomSizes = rightBloomMap.map { case (col, bloom) => s"$col -> ${bloom.bitSize()}" }.pretty
    logger.info(s"""
               |Label JoinPart Info:
               |  part name : ${joinPart.groupBy.metaData.name},
               |  left type : ${joinConf.left.dataModel},
               |  right type: ${joinPart.groupBy.dataModel},
               |  accuracy  : ${joinPart.groupBy.inferredAccuracy},
               |  part unfilled range: $leftRange,
               |  bloom sizes: $bloomSizes
               |  groupBy: ${joinPart.groupBy.toString}
               |""".stripMargin)

    val groupBy = GroupBy.from(joinPart.groupBy,
                               PartitionRange(labelDS, labelDS),
                               tableUtils,
                               computeDependency = true,
                               Option(rightBloomMap.toMap.asJava),
                               rightSkewFilter)

    val df = (joinConf.left.dataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
      case (EVENTS, ENTITIES, _) =>
        groupBy.snapshotEntities
      case (EVENTS, EVENTS, _) =>
        groupBy.snapshotEvents(leftRange)
      case (_, _, _) =>
        throw new IllegalArgumentException(
          s"Data model type ${joinConf.left.dataModel}:${joinPart.groupBy.dataModel} " +
            "not supported for label join. Valid type [Events : Entities] or [Events : Events]")
    }
    df.withColumnRenamed(tableUtils.partitionColumn, Constants.LabelPartitionColumn)
  }

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray
    // drop dup label_ds column if exists in leftDf.
    val updatedLeftDf = if (leftDf.columns.contains(Constants.LabelPartitionColumn)) {
      leftDf.drop(Constants.LabelPartitionColumn)
    } else {
      leftDf
    }

    // apply key-renaming to key columns
    val keyRenamedRight = joinPart.rightToLeft.foldLeft(rightDf) { case (updatedRight, (rightKey, leftKey)) =>
      updatedRight.withColumnRenamed(rightKey, leftKey)
    }

    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
                                                                     tableUtils.partitionColumn,
                                                                     Constants.TimePartitionColumn,
                                                                     Constants.LabelPartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)
    val prefixedRight = keyRenamedRight.prefixColumnNames(joinPart.columnPrefix, valueColumns)

    val partName = joinPart.groupBy.metaData.name

    logger.info(s"""Join keys for $partName: ${partLeftKeys.mkString(", ")}
               |Left Schema:
               |${updatedLeftDf.schema.pretty}
               |
               |Right Schema:
               |${prefixedRight.schema.pretty}
               |
               |""".stripMargin)

    updatedLeftDf.validateJoinKeys(prefixedRight, partLeftKeys)
    updatedLeftDf.join(prefixedRight, partLeftKeys, "left_outer")
  }
}
