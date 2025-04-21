package ai.chronon.spark.batch
import ai.chronon.api
import ai.chronon.api.DataModel.EVENTS
import ai.chronon.api.Extensions._
import ai.chronon.api.PartitionRange.toTimeRange
import ai.chronon.api._
import ai.chronon.online.metrics.Metrics
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.Extensions._
import ai.chronon.spark.GroupBy
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DataType, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.Seq

// let's say we are running the label join on `ds`, we want to modify partitions of the join output table
// that are `ds - windowLength` days old. window sizes could repeat across different label join parts
//
// so we create a struct to map which partitions of join output table to modify for each window size (AllLabelOutputInfo)
// and for each label join part which columns have that particular window size. (LabelPartOutputInfo)
//
// We keep a Seq of Windows on the LabelPartOutputInfo to help limit actual computation needed in the TemporalEvents case.
case class LabelPartOutputInfo(labelPart: JoinPart, outputColumnNames: Seq[String], windows: Seq[api.Window])
case class AllLabelOutputInfo(joinDsAsRange: PartitionRange, labelPartOutputInfos: Seq[LabelPartOutputInfo])

class LabelJoinV2(joinConf: api.Join, tableUtils: TableUtils, labelDateRange: api.DateRange) {
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
  private val labelDsAsPartitionRange = labelDateRange.toPartitionRange

  private def getLabelColSchema(labelOutputs: Seq[AllLabelOutputInfo]): Seq[(String, DataType)] = {
    val labelPartToOutputCols = labelOutputs
      .flatMap(_.labelPartOutputInfos)
      .groupBy(_.labelPart)
      .mapValues(_.flatMap(_.outputColumnNames))

    labelPartToOutputCols.flatMap { case (labelPart, outputCols) =>
      val gb = GroupBy.from(labelPart.groupBy, labelDsAsPartitionRange, tableUtils, computeDependency = false, None)
      val gbSchema = StructType(SparkConversions.fromChrononSchema(gb.outputSchema).fields)

      // The GroupBy Schema will not contain the labelPart prefix
      outputCols.map(col => (col, gbSchema(col.replace(s"${labelPart.fullPrefix}_", "")).dataType))
    }.toSeq
  }

  private def runAssertions(): Unit = {
    assert(joinConf.left.dataModel == DataModel.EVENTS,
           s"join.left.dataMode needs to be Events for label join ${joinConf.metaData.name}")

    assert(Option(joinConf.metaData.team).nonEmpty,
           s"join.metaData.team needs to be set for join ${joinConf.metaData.name}")

    labelJoinConf.labels.asScala.foreach { jp =>
      assert(jp.groupBy.dataModel == DataModel.EVENTS,
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
            agg.windows.asScala.map { w =>
              // TODO -- support buckets

              assert(Option(agg.buckets).isEmpty, "Buckets as labels are not yet supported in LabelJoinV2")
              val aggPart = Builders.AggregationPart(agg.operation, agg.inputColumn, w)

              // Sub day windows get bucketed into their day bucket, because that's what matters for calculating
              // The "backwards" looking window to find the join output to join against for a given day of label data
              // We cannot compute labelJoin for the same day as label data. For example, even for a 2-hour window,
              // events between 22:00-23:59 will not have complete values. So the minimum offset is 1 day.
              val effectiveLength = w.timeUnit match {
                case TimeUnit.DAYS    => w.length
                case TimeUnit.HOURS   => math.ceil(w.length / 24.0).toInt
                case TimeUnit.MINUTES => math.ceil(w.length / (24.0 * 60)).toInt
              }

              val fullColName = s"${labelJoinPart.fullPrefix}_${aggPart.outputColumnName}"

              (effectiveLength, fullColName, w)
            }
          }
          .groupBy(_._1)
          .map { case (window, windowAndOutputCols) =>
            (window, LabelPartOutputInfo(labelJoinPart, windowAndOutputCols.map(_._2), windowAndOutputCols.map(_._3)))
          }
      }
      .groupBy(_._1) // Flatten map and combine into one map with window as key
      .mapValues(_.map(_._2)) // Drop the duplicate window
      .map { case (window, labelPartOutputInfos) =>
        // The labelDs is a lookback from the labelSnapshot partition back to the join output table
        val joinPartitionDsAsRange = labelDsAsPartitionRange.shift(window * -1)
        window -> AllLabelOutputInfo(joinPartitionDsAsRange, labelPartOutputInfos)
      }
      .toMap
  }

  def compute(): DataFrame = {
    val resultDfsPerDay = labelDsAsPartitionRange.steps(days = 1).map { dayStep =>
      computeDay(dayStep.start)
    }

    resultDfsPerDay.tail.foldLeft(resultDfsPerDay.head)((acc, df) => acc.union(df))
  }

  def computeDay(labelDs: String): DataFrame = {
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

      // Always log this no matter what.
      val baseLogString = s"""Missing following partitions from $joinTable: ${missingWindowToOutputs.values
        .map(_.joinDsAsRange.start)
        .mkString(", ")}
           |
           |Found existing partitions of join output: ${existingJoinPartitions.mkString(", ")}
           |
           |Required dates are computed based on label date (the run date) - window for distinct windows that are used in label parts.
           |
           |In this case, the run date is: $labelDs, and given the existing partitions we are unable to compute the labels for the following windows: ${missingWindowToOutputs.keys
        .mkString(", ")} (days).
           |
           |""".stripMargin

      // If there are no dates to run, also throw that error
      require(
        computableWindowToOutputs.nonEmpty,
        s"""$baseLogString
           |
           |There are no partitions that we can run the label join for. At least one window must be computable.
           |
           |Exiting.
           |""".stripMargin
      )

      // Else log what we are running, but warn about missing windows
      logger.warn(
        s"""$baseLogString
           |
           |Proceeding with valid windows: ${computableWindowToOutputs.keys.mkString(", ")}
           |
           |""".stripMargin
      )
    }

    // Find existing partition in the outputLabelTable (different from the join output table used above)
    // This is used below in computing baseJoinDf
    val existingLabelTableOutputPartitions = tableUtils.partitions(outputLabelTable)
    logger.info(s"Found existing partitions in Label Table: ${existingLabelTableOutputPartitions.mkString(", ")}")

    // Each unique window is an output partition in the joined table
    // Each window may contain a subset of the joinParts and their columns
    computableWindowToOutputs.foreach { case (windowLength, joinOutputInfo) =>
      computeOutputForWindow(windowLength,
                             joinOutputInfo,
                             existingLabelTableOutputPartitions,
                             windowToLabelOutputInfos,
                             labelDsAsPartitionRange)
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
                                     windowToLabelOutputInfos: Map[Int, AllLabelOutputInfo],
                                     labelDsAsPartitionRange: PartitionRange): Unit = {
    logger.info(
      s"Computing labels for window: $windowLength days on labelDs: ${labelDsAsPartitionRange.start} \n" +
        s"Includes the following joinParts and output cols: ${joinOutputInfo.labelPartOutputInfos
          .map(x => s"${x.labelPart.groupBy.metaData.name} -> ${x.outputColumnNames.mkString(", ")}")
          .mkString("\n")}")

    val startMillis = System.currentTimeMillis()
    // This is the join output ds that we're working with
    val joinDsAsRange = labelDsAsPartitionRange.shift(windowLength * -1)

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

    // Cache the left DF because it's used multiple times in offsetting in the case that there are Temporal Events
    if (joinOutputInfo.labelPartOutputInfos.exists(_.labelPart.groupBy.dataModel == Accuracy.TEMPORAL)) {
      joinBaseDf.cache()
    }

    val joinPartsAndDfs = joinOutputInfo.labelPartOutputInfos.map { labelOutputInfo =>
      val labelJoinPart = labelOutputInfo.labelPart
      val groupByConf = labelJoinPart.groupBy
      // In the case of multiple sub-day windows within the day offset (i.e. 6hr, 12hr, 1day), we get multiple output dfs
      // Snapshot accuracy never has sub-day windows
      // Temporal accuracy may also only have one, if there are not multiple sub-day windows
      val rightDfs: Seq[DataFrame] =
        (joinConf.left.dataModel, groupByConf.dataModel, groupByConf.inferredAccuracy) match {
          case (EVENTS, EVENTS, Accuracy.SNAPSHOT) =>
            // In the snapshot Accuracy case we join against the snapshot table
            val outputColumnNames =
              labelOutputInfo.outputColumnNames.map(_.replace(s"${labelJoinPart.fullPrefix}_", ""))
            // Rename the value columns from the SnapshotTable to include prefix
            val selectCols: Map[String, String] =
              (labelJoinPart.rightToLeft.keys ++ outputColumnNames).map(x => x -> x).toMap

            val snapshotQuery = Builders.Query(selects = selectCols)
            val snapshotTable = groupByConf.metaData.outputTable

            Seq(tableUtils.scanDf(snapshotQuery, snapshotTable, range = Some(labelDsAsPartitionRange)))

          case (EVENTS, EVENTS, Accuracy.TEMPORAL) =>
            // We shift the left timestamps by window length and call `GroupBy.temporalEvents` to compute a PITC join. We do this once per window length within the GroupBy, and join all the returned data-frames back.
            computeTemporalLabelJoinPart(joinBaseDf, joinDsAsRange, groupByConf, labelOutputInfo)

          case (_, _, _) =>
            throw new NotImplementedError(
              "LabelJoin is currently only supported with Events on the Left of the Join, and as the GroupBy source for labels")
        }

      (labelJoinPart, rightDfs)
    }

    val joined = joinPartsAndDfs.foldLeft(joinBaseDf) { case (left, (joinPart, rightDfs)) =>
      rightDfs.foldLeft(left) { (currentLeft, rightDf) =>
        joinWithLeft(currentLeft, rightDf, joinPart)
      }
    }

    val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)

    metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)

    joined.save(outputLabelTable, confTableProps, Seq(tableUtils.partitionColumn), autoExpand = true)

    logger.info(s"Wrote to table $outputLabelTable, into partitions: ${joinDsAsRange.start} in $elapsedMins mins")
  }

  def computeTemporalLabelJoinPart(joinBaseDf: DataFrame,
                                   joinDsAsRange: PartitionRange,
                                   groupByConf: api.GroupBy,
                                   labelOutputInfo: LabelPartOutputInfo): Seq[DataFrame] = {

    // 1-day and sub-day windows get processed to the same output partition, however, we need to handle the offset
    // differently for each one. So here we compute a dataframe for each window in the 1-day offset and join
    // Them together into a single dataframe
    labelOutputInfo.windows.map { w =>
      val minutesOffset = w.timeUnit match {
        case TimeUnit.DAYS    => w.length * 60 * 24
        case TimeUnit.HOURS   => w.length * 60
        case TimeUnit.MINUTES => w.length
      }

      val millisOffset = minutesOffset * 60 * 1000L

      // Shift the left timestamp forward so that backwards looking temporal computation results in forward looking window
      val shiftedLeftDf = joinBaseDf.withColumn(Constants.TimeColumn, col(Constants.TimeColumn) + lit(millisOffset))

      // Use the shifted partition range to ensure correct scan on the right side data for temporal compute
      // OffsetWindowLength is the rounded-up day window (corresponds to delta between labels and join output)
      // In the case of sub-day windows, the shifted range will now span an extra day.
      val shiftedLeftPartitionRange = joinDsAsRange.shiftMillis(millisOffset)

      logger.info(
        s"Computing temporal label join for ${labelOutputInfo.labelPart.groupBy.metaData.name} with shifted range: $shiftedLeftPartitionRange and ")

      val gb = genGroupBy(groupByConf, shiftedLeftPartitionRange, w)

      val temporalDf = gb.temporalEvents(shiftedLeftDf, Some(toTimeRange(shiftedLeftPartitionRange)))

      // Now shift time back so it lines up with left (unfortunately, preserving both columns could require a change
      // to temporalEvents engine -- best avoided)
      temporalDf.withColumn(Constants.TimeColumn, col(Constants.TimeColumn) - lit(millisOffset))
    }
  }

  def genGroupBy(groupByConf: api.GroupBy, partitionRange: PartitionRange, window: api.Window): GroupBy = {

    // Remove other windows to not over-compute
    val filteredGroupByConf = filterGroupByWindows(groupByConf, window)

    // TODO: Implement bloom filter if needed
    // val bloomFilter = JoinUtils.genBloomFilterIfNeeded(joinPart, leftDataModel, partitionRange, None)

    GroupBy.from(filteredGroupByConf, partitionRange, tableUtils, computeDependency = true, None)
  }

  def filterGroupByWindows(groupBy: api.GroupBy, keepWindow: api.Window): api.GroupBy = {
    // Modifies a GroupBy to only keep the windows that are in the keepWindows list
    val gb = groupBy.deepCopy()

    if (gb.aggregations == null) return gb

    val filteredAggs = gb.aggregations.asScala
      .filter { agg => agg.windows != null && agg.windows.asScala.contains(keepWindow) }
      .map { agg => agg.setWindows(Seq(keepWindow).asJava) }

    gb.setAggregations(filteredAggs.asJava)
  }

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    // compute join keys, besides the groupBy keys -  like ds, ts etc.,
    val isTemporal = joinPart.groupBy.inferredAccuracy == Accuracy.TEMPORAL
    val partLeftKeys =
      joinPart.rightToLeft.values.toArray ++ (if (isTemporal) Seq(Constants.TimeColumn, tableUtils.partitionColumn)
                                              else Seq.empty)

    // apply key-renaming to key columns
    val keyRenamedRight = joinPart.rightToLeft.foldLeft(rightDf) { case (updatedRight, (rightKey, leftKey)) =>
      updatedRight.withColumnRenamed(rightKey, leftKey)
    }

    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
                                                                     tableUtils.partitionColumn,
                                                                     Constants.TimePartitionColumn,
                                                                     Constants.LabelPartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)

    val fullPrefix = s"${labelColumnPrefix}_${joinPart.fullPrefix}"

    // In this case, since we're joining with the full-schema dataframe,
    // we need to drop the columns that we're attempting to overwrite
    val cleanLeftDf = valueColumns.foldLeft(leftDf)((df, colName) => df.drop(s"${fullPrefix}_$colName"))

    val prefixedRight = keyRenamedRight.prefixColumnNames(fullPrefix, valueColumns)

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
