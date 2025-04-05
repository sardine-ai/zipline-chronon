package ai.chronon.spark.batch
import ai.chronon.api
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.Metrics
import ai.chronon.spark.Extensions._
import ai.chronon.spark.TableUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.Seq

// let's say we are running the label join on `ds`, we want to modify partitions of the join output table
// that are `ds - windowLength` days old. window sizes could repeat across different label join parts
//
// so we create a struct to map which partitions of join output table to modify for each window size (AllLabelOutputInfo)
// and for each label join part which columns have that particular window size. (LabelPartOutputInfo)
case class LabelPartOutputInfo(labelPart: JoinPart, outputColumnNames: Seq[String])
case class AllLabelOutputInfo(joinDsAsRange: PartitionRange, labelPartOutputInfos: Seq[LabelPartOutputInfo])

class LabelJoinV2(joinConf: api.Join, tableUtils: TableUtils, labelDs: String) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
  assert(Option(joinConf.metaData.outputNamespace).nonEmpty, "output namespace could not be empty or null")

  val metrics: Metrics.Context = Metrics.Context(Metrics.Environment.LabelJoin, joinConf)
  private val outputLabelTable = joinConf.metaData.outputLabelTableV2
  private val labelJoinConf = joinConf.labelParts
  private val confTableProps = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])
  private val labelColumnPrefix = "label_"
  private val labelDsAsRange = PartitionRange(labelDs, labelDs)

  private def getLabelColSchema(labelOutputs: Seq[AllLabelOutputInfo]): Seq[(String, DataType)] = {
    val labelPartToOutputCols = labelOutputs
      .flatMap(_.labelPartOutputInfos)
      .groupBy(_.labelPart)
      .mapValues(_.flatMap(_.outputColumnNames))

    labelPartToOutputCols.flatMap { case (labelPart, outputCols) =>
      val labelPartSchema = tableUtils.scanDf(null, labelPart.groupBy.metaData.outputTable).schema
      outputCols.map(col => (col, labelPartSchema(col).dataType))
    }.toSeq
  }

  private def runAssertions(): Unit = {
    assert(joinConf.left.dataModel == Events,
           s"join.left.dataMode needs to be Events for label join ${joinConf.metaData.name}")

    assert(Option(joinConf.metaData.team).nonEmpty,
           s"join.metaData.team needs to be set for join ${joinConf.metaData.name}")

    labelJoinConf.labels.asScala.foreach { jp =>
      assert(jp.groupBy.dataModel == Events,
             s"groupBy.dataModel must be Events for label join with aggregations ${jp.groupBy.metaData.name}")

      assert(Option(jp.groupBy.aggregations).isDefined,
             s"aggregations must be defined for label join ${jp.groupBy.metaData.name}")

      val windows = jp.groupBy.aggregations.asScala.flatMap(_.windows.asScala).filter(_.timeUnit == TimeUnit.DAYS)

      assert(windows.nonEmpty,
             s"at least one aggregation with a daily window must be defined for label join ${jp.groupBy.metaData.name}")
    }
  }

  private def getWindowToLabelOutputInfos: Map[Int, AllLabelOutputInfo] = {
    // Create a map of window to LabelOutputInfo
    // Each window could be shared across multiple labelJoinParts
    val labelJoinParts = labelJoinConf.labels.asScala

    labelJoinParts
      .flatMap { labelJoinPart =>
        labelJoinPart.groupBy.aggregations.asScala
          .flatMap { agg =>
            agg.windows.asScala.filter(_.timeUnit == TimeUnit.DAYS).map { w =>
              // TODO -- support buckets
              assert(Option(agg.buckets).isEmpty, "Buckets as labels are not yet supported in LabelJoinV2")
              val aggPart = Builders.AggregationPart(agg.operation, agg.inputColumn, w)
              (w.length, aggPart.outputColumnName)
            }
          }
          .groupBy(_._1)
          .map { case (window, windowAndOutputCols) =>
            (window, LabelPartOutputInfo(labelJoinPart, windowAndOutputCols.map(_._2)))
          }
      }
      .groupBy(_._1) // Flatten map and combine into one map with window as key
      .mapValues(_.map(_._2)) // Drop the duplicate window
      .map { case (window, labelPartOutputInfos) =>
        // The labelDs is a lookback from the labelSnapshot partition back to the join output table
        val joinPartitionDsAsRange = labelDsAsRange.shift(window * -1)
        window -> AllLabelOutputInfo(joinPartitionDsAsRange, labelPartOutputInfos)
      }
      .toMap
  }

  def compute(): DataFrame = {
    logger.info(s"Running LabelJoinV2 for $labelDs")

    runAssertions()

    // First get a map of window to LabelOutputInfo
    val windowToLabelOutputInfos = getWindowToLabelOutputInfos

    // Find existing partition in the join table
    val joinTable = joinConf.metaData.outputTable
    val existingJoinPartitions = tableUtils.partitions(joinTable)

    // Split the windows into two groups, one that has a corresponding partition in the join table and one that doesn't
    // If a partition is missing, we can't compute the labels for that window, but the job will proceed with the rest
    val (computableWindowToOutputs, missingWindowToOutputs) = windowToLabelOutputInfos.partition {
      case (_, labelOutputInfo) =>
        existingJoinPartitions.contains(labelOutputInfo.joinDsAsRange.start)
    }

    if (missingWindowToOutputs.nonEmpty) {
      logger.info(
        s"""Missing following partitions from $joinTable: ${missingWindowToOutputs.values
          .map(_.joinDsAsRange.start)
          .mkString(", ")}
           |
           |Found existing partitions: ${existingJoinPartitions.mkString(", ")}
           |
           |Therefore unable to compute the labels for ${missingWindowToOutputs.keys.mkString(", ")}
           |
           |For requested ds: $labelDs
           |
           |Proceeding with valid windows: ${computableWindowToOutputs.keys.mkString(", ")}
           |
           |""".stripMargin
      )

      require(
        computableWindowToOutputs.isEmpty,
        "No valid windows to compute labels for given the existing join output range." +
          s"Consider backfilling the join output table for the following days: ${missingWindowToOutputs.values.map(_.joinDsAsRange.start)}."
      )
    }

    // Find existing partition in the outputLabelTable (different from the join output table used above)
    // This is used below in computing baseJoinDf
    val existingLabelTableOutputPartitions = tableUtils.partitions(outputLabelTable)
    logger.info(s"Found existing partitions in Label Table: ${existingLabelTableOutputPartitions.mkString(", ")}")

    // Each unique window is an output partition in the joined table
    // Each window may contain a subset of the joinParts and their columns
    computableWindowToOutputs.foreach { case (windowLength, joinOutputInfo) =>
      computeOutputForWindow(windowLength, joinOutputInfo, existingLabelTableOutputPartitions, windowToLabelOutputInfos)
    }

    val allOutputDfs = computableWindowToOutputs.values
      .map(_.joinDsAsRange)
      .map { range =>
        tableUtils.scanDf(null, outputLabelTable, range = Some(range))
      }
      .toSeq

    if (allOutputDfs.length == 1) {
      allOutputDfs.head
    } else {
      allOutputDfs.reduce(_ union _)
    }
  }

  // Writes out a single partition of the label table with all labels for the corresponding window
  private def computeOutputForWindow(windowLength: Int,
                                     joinOutputInfo: AllLabelOutputInfo,
                                     existingLabelTableOutputPartitions: Seq[String],
                                     windowToLabelOutputInfos: Map[Int, AllLabelOutputInfo]): Unit = {
    logger.info(
      s"Computing labels for window: $windowLength days on labelDs: $labelDs \n" +
        s"Includes the following joinParts and output cols: ${joinOutputInfo.labelPartOutputInfos
          .map(x => s"${x.labelPart.groupBy.metaData.name} -> ${x.outputColumnNames.mkString(", ")}")
          .mkString("\n")}")

    val startMillis = System.currentTimeMillis()
    // This is the join output ds that we're working with
    val joinDsAsRange = labelDsAsRange.shift(windowLength * -1)

    val joinBaseDf = if (existingLabelTableOutputPartitions.contains(joinDsAsRange.start)) {
      // If the existing join table has the partition, then we should use it, because another label column
      // may have landed for this date, otherwise we can use the base join output and
      logger.info(s"Found existing partition in Label Table: ${joinDsAsRange.start}")
      tableUtils.scanDf(null, outputLabelTable, range = Some(joinDsAsRange))
    } else {
      // Otherwise we need to use the join output, but pad the schema to include other label columns that might
      // be on the schema
      logger.info(s"Did not find existing partition in Label Table, querying from Join Output: ${joinDsAsRange.start}")
      val joinOutputDf = tableUtils.scanDf(null, joinConf.metaData.outputTable, range = Some(joinDsAsRange))
      val allLabelCols = getLabelColSchema(windowToLabelOutputInfos.values.toSeq)
      allLabelCols.foldLeft(joinOutputDf) { case (currentDf, (colName, dataType)) =>
        val prefixedColName = s"${labelColumnPrefix}_$colName"
        currentDf.withColumn(prefixedColName, lit(null).cast(dataType))
      }
    }

    val joinPartsAndDfs = joinOutputInfo.labelPartOutputInfos.map { labelOutputInfo =>
      val labelJoinPart = labelOutputInfo.labelPart

      val outputColumnNames = labelOutputInfo.outputColumnNames
      val selectCols: Map[String, String] =
        (labelJoinPart.rightToLeft.keys ++ outputColumnNames).map(x => x -> x).toMap

      val snapshotQuery = Builders.Query(selects = selectCols)
      val snapshotTable = labelJoinPart.groupBy.metaData.outputTable
      val snapshotDf = tableUtils.scanDf(snapshotQuery, snapshotTable, range = Some(labelDsAsRange))

      (labelJoinPart, snapshotDf)
    }

    val joined = joinPartsAndDfs.foldLeft(joinBaseDf) { case (left, (joinPart, rightDf)) =>
      joinWithLeft(left, rightDf, joinPart)
    }

    val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)

    metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)

    joined.save(outputLabelTable, confTableProps, Seq(tableUtils.partitionColumn), autoExpand = true)

    logger.info(s"Wrote to table $outputLabelTable, into partitions: ${joinDsAsRange.start} in $elapsedMins mins")
  }

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray

    // apply key-renaming to key columns
    val keyRenamedRight = joinPart.rightToLeft.foldLeft(rightDf) { case (updatedRight, (rightKey, leftKey)) =>
      updatedRight.withColumnRenamed(rightKey, leftKey)
    }

    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
                                                                     tableUtils.partitionColumn,
                                                                     Constants.TimePartitionColumn,
                                                                     Constants.LabelPartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)

    // In this case, since we're joining with the full-schema dataframe,
    // we need to drop the columns that we're attempting to overwrite
    val cleanLeftDf = valueColumns.foldLeft(leftDf)((df, colName) => df.drop(s"${labelColumnPrefix}_$colName"))

    val prefixedRight = keyRenamedRight.prefixColumnNames(labelColumnPrefix, valueColumns)

    val partName = joinPart.groupBy.metaData.name

    logger.info(s"""Join keys for $partName: ${partLeftKeys.mkString(", ")}
                   |Left Schema:
                   |${leftDf.schema.pretty}
                   |
                   |Right Schema:
                   |${prefixedRight.schema.pretty}
                   |
                   |""".stripMargin)

    cleanLeftDf.validateJoinKeys(prefixedRight, partLeftKeys)
    cleanLeftDf.join(prefixedRight, partLeftKeys, "left_outer")
  }
}
