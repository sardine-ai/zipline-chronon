package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.{Accuracy, Constants, DateRange, JoinPart, PartitionRange, PartitionSpec}
import ai.chronon.api.DataModel.{DataModel, Entities, Events}
import ai.chronon.api.Extensions.{DateRangeOps, DerivationOps, GroupByOps, JoinPartOps, MetadataOps}
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.orchestration.JoinPartNode

import ai.chronon.online.Metrics
import ai.chronon.spark.Extensions.DfWithStats
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.collection.Seq
import scala.collection.Map
import java.util

case class JoinPartJobContext(leftDf: Option[DfWithStats],
                              joinLevelBloomMapOpt: Option[util.Map[String, BloomFilter]],
                              leftTimeRangeOpt: Option[PartitionRange],
                              tableProps: Map[String, String],
                              runSmallMode: Boolean)

class JoinPartJob(node: JoinPartNode, range: DateRange, showDf: Boolean = false)(implicit tableUtils: TableUtils) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val partitionSpec = tableUtils.partitionSpec

  private val leftTable = node.leftSourceTable
  private val leftDataModel = node.leftDataModel match {
    case "Entities" => Entities
    case "Events"   => Events
  }
  private val joinPart = node.joinPart
  private val dateRange = range.toPartitionRange
  private val skewKeys: Option[Map[String, Seq[String]]] = Option(node.skewKeys).map { skewKeys =>
    skewKeys.asScala.map { case (k, v) => k -> v.asScala.toSeq }.toMap
  }

  def run(context: Option[JoinPartJobContext] = None): Option[DataFrame] = {

    val jobContext = context.getOrElse {
      // LeftTable is already computed by SourceJob, no need to apply query/filters/etc
      val cachedLeftDf = tableUtils.scanDf(query = null, leftTable, range = Some(dateRange))

      val leftTimeRangeOpt: Option[PartitionRange] =
        if (cachedLeftDf.schema.fieldNames.contains(Constants.TimePartitionColumn)) {
          val leftTimePartitionMinMax = cachedLeftDf.range[String](Constants.TimePartitionColumn)
          Some(PartitionRange(leftTimePartitionMinMax._1, leftTimePartitionMinMax._2))
        } else {
          None
        }

      val runSmallMode = JoinUtils.runSmallMode(tableUtils, cachedLeftDf)

      val leftWithStats = cachedLeftDf.withStats

      val joinLevelBloomMapOpt =
        JoinUtils.genBloomFilterIfNeeded(joinPart, leftDataModel, cachedLeftDf.count, dateRange, None)

      JoinPartJobContext(Option(leftWithStats),
                         joinLevelBloomMapOpt,
                         leftTimeRangeOpt,
                         Map.empty[String, String],
                         runSmallMode)
    }

    // TODO: fix left df and left time range, bloom filter, small mode args
    computeRightTable(
      jobContext.leftDf,
      joinPart,
      dateRange,
      jobContext.leftTimeRangeOpt,
      node.metaData.outputTable,
      jobContext.tableProps,
      jobContext.joinLevelBloomMapOpt,
      jobContext.runSmallMode
    )
  }

  def computeRightTable(leftDfOpt: Option[DfWithStats],
                        joinPart: JoinPart,
                        leftRange: PartitionRange, // missing left partitions
                        leftTimeRangeOpt: Option[PartitionRange], // range of timestamps within missing left partitions
                        partTable: String,
                        tableProps: Map[String, String] = Map(),
                        joinLevelBloomMapOpt: Option[util.Map[String, BloomFilter]],
                        smallMode: Boolean = false): Option[DataFrame] = {

    // val partMetrics = Metrics.Context(metrics, joinPart) -- TODO is this metrics context sufficient, or should we pass thru for monolith join?
    val partMetrics = Metrics.Context(Metrics.Environment.JoinOffline, joinPart.groupBy)

    val rightRange = JoinUtils.shiftDays(leftDataModel, joinPart, leftTimeRangeOpt, leftDfOpt, leftRange)

    // Can kill the option after we deprecate monolith join job
    leftDfOpt.map { leftDf =>
      try {
        val start = System.currentTimeMillis()
        val prunedLeft = leftDf.prunePartitions(leftRange) // We can kill this after we deprecate monolith join job
        val filledDf =
          computeJoinPart(prunedLeft, joinPart, joinLevelBloomMapOpt, skipBloom = smallMode)
        // Cache join part data into intermediate table
        if (filledDf.isDefined) {
          logger.info(s"Writing to join part table: $partTable for partition range $rightRange")
          filledDf.get.save(partTable,
                            tableProps.toMap,
                            stats = prunedLeft.map(_.stats),
                            sortByCols = joinPart.groupBy.keyColumns.toScala)
        } else {
          logger.info(s"Skipping $partTable because no data in computed joinPart.")
        }
        val elapsedMins = (System.currentTimeMillis() - start) / 60000
        partMetrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
        val partitionCount = rightRange.partitions.length
        partMetrics.gauge(Metrics.Name.PartitionCount, partitionCount)
        logger.info(s"Wrote $partitionCount partitions to join part table: $partTable in $elapsedMins minutes")
      } catch {
        case e: Exception =>
          logger.error(s"Error while processing groupBy: ${joinPart.groupBy.getMetaData.getName}")
          throw e
      }
    }

    if (tableUtils.tableReachable(partTable)) {
      Some(tableUtils.scanDf(query = null, partTable, range = Some(rightRange)))
    } else {
      // Happens when everything is handled by bootstrap
      None
    }
  }

  private def computeJoinPart(leftDfWithStats: Option[DfWithStats],
                              joinPart: JoinPart,
                              joinLevelBloomMapOpt: Option[util.Map[String, BloomFilter]],
                              skipBloom: Boolean): Option[DataFrame] = {

    if (leftDfWithStats.isEmpty) {
      // happens when all rows are already filled by bootstrap tables
      logger.info(s"\nBackfill is NOT required for ${joinPart.groupBy.metaData.name} since all rows are bootstrapped.")
      return None
    }

    val statsDf = leftDfWithStats.get
    val rowCount = statsDf.count
    val unfilledRange = statsDf.partitionRange

    logger.info(
      s"\nBackfill is required for ${joinPart.groupBy.metaData.name} for $rowCount rows on range $unfilledRange")
    val rightBloomMap = if (skipBloom) {
      None
    } else {
      JoinUtils.genBloomFilterIfNeeded(joinPart, leftDataModel, rowCount, unfilledRange, joinLevelBloomMapOpt)
    }

    val rightSkewFilter = JoinUtils.partSkewFilter(joinPart, skewKeys)

    def genGroupBy(partitionRange: PartitionRange) =
      GroupBy.from(joinPart.groupBy,
                   partitionRange,
                   tableUtils,
                   computeDependency = true,
                   rightBloomMap,
                   rightSkewFilter,
                   showDf = showDf)

    // all lazy vals - so evaluated only when needed by each case.
    lazy val partitionRangeGroupBy = genGroupBy(unfilledRange)

    lazy val unfilledTimeRange = {
      val timeRange = statsDf.timeRange
      logger.info(s"left unfilled time range: $timeRange")
      timeRange
    }

    val leftSkewFilter =
      JoinUtils.skewFilter(Option(joinPart.rightToLeft.values.toSeq), skewKeys, joinPart.rightToLeft.values.toSeq)
    // this is the second time we apply skew filter - but this filters only on the keys
    // relevant for this join part.
    println("leftSkewFilter: " + leftSkewFilter)
    lazy val skewFilteredLeft = leftSkewFilter
      .map { sf =>
        val filtered = statsDf.df.filter(sf)
        logger.info(s"""Skew filtering left-df for
                       |GroupBy: ${joinPart.groupBy.metaData.name}
                       |filterClause: $sf
                       |""".stripMargin)
        filtered
      }
      .getOrElse(statsDf.df)

    /*
      For the corner case when the values of the key mapping also exist in the keys, for example:
      Map(user -> user_name, user_name -> user)
      the below logic will first rename the conflicted column with some random suffix and update the rename map
     */
    lazy val renamedLeftRawDf = {
      val columns = skewFilteredLeft.columns.flatMap { column =>
        if (joinPart.leftToRight.contains(column)) {
          Some(col(column).as(joinPart.leftToRight(column)))
        } else if (joinPart.rightToLeft.contains(column)) {
          None
        } else {
          Some(col(column))
        }
      }
      skewFilteredLeft.select(columns: _*)
    }

    lazy val shiftedPartitionRange = unfilledTimeRange.toPartitionRange.shift(-1)

    val renamedLeftDf = renamedLeftRawDf.select(renamedLeftRawDf.columns.map {
      case c if c == tableUtils.partitionColumn =>
        date_format(renamedLeftRawDf.col(c), tableUtils.partitionFormat).as(c)
      case c => renamedLeftRawDf.col(c)
    }.toList: _*)
    val rightDf = (leftDataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
      case (Entities, Events, _)   => partitionRangeGroupBy.snapshotEvents(unfilledRange)
      case (Entities, Entities, _) => partitionRangeGroupBy.snapshotEntities
      case (Events, Events, Accuracy.SNAPSHOT) =>
        genGroupBy(shiftedPartitionRange).snapshotEvents(shiftedPartitionRange)
      case (Events, Events, Accuracy.TEMPORAL) =>
        genGroupBy(unfilledTimeRange.toPartitionRange).temporalEvents(renamedLeftDf, Some(unfilledTimeRange))

      case (Events, Entities, Accuracy.SNAPSHOT) => genGroupBy(shiftedPartitionRange).snapshotEntities

      case (Events, Entities, Accuracy.TEMPORAL) =>
        // Snapshots and mutations are partitioned with ds holding data between <ds 00:00> and ds <23:59>.
        genGroupBy(shiftedPartitionRange).temporalEntities(renamedLeftDf)
    }
    val rightDfWithDerivations = if (joinPart.groupBy.hasDerivations) {
      val finalOutputColumns = joinPart.groupBy.derivationsScala.finalOutputColumn(rightDf.columns).toSeq
      val result = rightDf.select(finalOutputColumns: _*)
      result
    } else {
      rightDf
    }
    if (showDf) {
      logger.info(s"printing results for joinPart: ${joinPart.groupBy.metaData.name}")
      rightDfWithDerivations.prettyPrint()
    }
    Some(rightDfWithDerivations)
  }
}
