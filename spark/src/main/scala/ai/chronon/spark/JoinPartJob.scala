package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Accuracy
import ai.chronon.api.Constants
import ai.chronon.api.DataModel.Entities
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions.DerivationOps
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.JoinPartOps
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.JoinPart
import ai.chronon.api.PartitionSpec
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.online.Metrics
import ai.chronon.online.PartitionRange
import ai.chronon.spark.Extensions.DfWithStats
import ai.chronon.spark.Extensions._
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util

case class JoinPartJobContext(leftDf: Option[DfWithStats],
                              joinLevelBloomMapOpt: Option[util.Map[String, BloomFilter]],
                              partTable: String,
                              leftTimeRangeOpt: Option[PartitionRange],
                              tableProps: Map[String, String],
                              runSmallMode: Boolean)

class JoinPartJob(left: api.Source,
                  joinPart: api.JoinPart,
                  dateRange: PartitionRange,
                  tableUtils: TableUtils,
                  skewKeys: Option[Map[String, Seq[String]]] = None,
                  overrideLeftTable: Option[String] = None,
                  showDf: Boolean = false) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  def run(context: Option[JoinPartJobContext] = None): Option[DataFrame] = {

    val jobContext = context.getOrElse {
      val cachedLeftDf = overrideLeftTable
        .map { table =>
          logger.info("Overwriting left side to use saved Bootstrap table...")
          left.overwriteTable(table)
          val query = left.query
          // sets map and where clauses already applied to bootstrap transformation
          query.setSelects(null)
          query.setWheres(null)
          val leftSkewFilter =
            JoinUtils.skewFilter(Some(joinPart.rightToLeft.values.toSeq), skewKeys, joinPart.rightToLeft.values.toSeq)
          JoinUtils.leftDfFromSource(left, dateRange, tableUtils, skewFilter = leftSkewFilter).get
        }
        .getOrElse(getCachedLeftDF())

      val leftTimeRangeOpt: Option[PartitionRange] =
        if (cachedLeftDf.schema.fieldNames.contains(Constants.TimePartitionColumn)) {
          val leftTimePartitionMinMax = cachedLeftDf.range[String](Constants.TimePartitionColumn)
          Some(PartitionRange(leftTimePartitionMinMax._1, leftTimePartitionMinMax._2))
        } else {
          None
        }

      val runSmallMode = JoinUtils.runSmallMode(tableUtils, cachedLeftDf)

      val leftOptWithStats = Option(cachedLeftDf.withStats)

      val joinLevelBloomMapOpt =
        JoinUtils.genBloomFilterIfNeeded(joinPart, left.dataModel, cachedLeftDf.count, dateRange, None)
      val partTable =
        (Seq(joinPart.groupBy.metaData.cleanName) ++ ThriftJsonCodec.md5Digest(left))
          .mkString("_") // TODO add left input table?
      JoinPartJobContext(leftOptWithStats,
                         joinLevelBloomMapOpt,
                         partTable,
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
      jobContext.partTable,
      jobContext.tableProps,
      jobContext.joinLevelBloomMapOpt,
      jobContext.runSmallMode
    )
  }

  def getCachedLeftDF(): DataFrame = {
    // TODO -- hardcoded ds below, namespacing on table, etc
    tableUtils.sql(f"SELECT * FROM ${left.getEvents.getTable}_${ThriftJsonCodec.md5Digest(
      left)} where ds BETWEEN ${dateRange.start} AND ${dateRange.end}")
  }

  def computeRightTable(leftDf: Option[DfWithStats],
                        joinPart: JoinPart,
                        leftRange: PartitionRange, // missing left partitions
                        leftTimeRangeOpt: Option[PartitionRange], // range of timestamps within missing left partitions
                        partTable: String,
                        tableProps: Map[String, String] = Map(),
                        joinLevelBloomMapOpt: Option[util.Map[String, BloomFilter]],
                        smallMode: Boolean = false): Option[DataFrame] = {

    // val partMetrics = Metrics.Context(metrics, joinPart) -- TODO is this metrics context sufficient, or should we pass thru for monolith join?
    val partMetrics = Metrics.Context(Metrics.Environment.JoinOffline, joinPart.groupBy)

    // in Events <> batch GB case, the partition dates are offset by 1
    val shiftDays =
      if (left.dataModel == Events && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
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
    val rightRange =
      if (left.dataModel == Events && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
        leftTimeRangeOpt.get.shift(shiftDays)
      } else {
        leftRange
      }

    try {
      val unfilledRanges = tableUtils
        .unfilledRanges(
          partTable,
          rightRange,
          Some(Seq(left.table)),
          inputToOutputShift = shiftDays,
          // never skip hole during partTable's range determination logic because we don't want partTable
          // and joinTable to be out of sync. skipping behavior is already handled in the outer loop.
          skipFirstHole = false
        )
        .getOrElse(Seq())

      val unfilledRangeCombined = if (unfilledRanges.nonEmpty && smallMode) {
        // For small mode we want to "un-chunk" the unfilled ranges, because left side can be sparse
        // in dates, and it often ends up being less efficient to run more jobs in an effort to
        // avoid computing unnecessary left range. In the future we can look for more intelligent chunking
        // as an alternative/better way to handle this.
        Seq(PartitionRange(unfilledRanges.minBy(_.start).start, unfilledRanges.maxBy(_.end).end))
      } else {
        unfilledRanges
      }

      val partitionCount = unfilledRangeCombined.map(_.partitions.length).sum
      if (partitionCount > 0) {
        val start = System.currentTimeMillis()
        unfilledRangeCombined
          .foreach(unfilledRange => {
            val leftUnfilledRange = unfilledRange.shift(-shiftDays)
            val prunedLeft = leftDf.flatMap(_.prunePartitions(leftUnfilledRange))
            val filledDf =
              computeJoinPart(prunedLeft, joinPart, joinLevelBloomMapOpt, skipBloom = smallMode)
            // Cache join part data into intermediate table
            if (filledDf.isDefined) {
              logger.info(s"Writing to join part table: $partTable for partition range $unfilledRange")
              filledDf.get.save(partTable,
                                tableProps,
                                stats = prunedLeft.map(_.stats),
                                sortByCols = joinPart.groupBy.keyColumns.toScala)
            } else {
              logger.info(s"Skipping $partTable because no data in computed joinPart.")
            }
          })
        val elapsedMins = (System.currentTimeMillis() - start) / 60000
        partMetrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
        partMetrics.gauge(Metrics.Name.PartitionCount, partitionCount)
        logger.info(s"Wrote $partitionCount partitions to join part table: $partTable in $elapsedMins minutes")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error while processing groupBy: ${joinPart.groupBy.getMetaData.getName}")
        throw e
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
      JoinUtils.genBloomFilterIfNeeded(joinPart, left.dataModel, rowCount, unfilledRange, joinLevelBloomMapOpt)
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
    val rightDf = (left.dataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
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
      val finalOutputColumns = joinPart.groupBy.derivationsScala.finalOutputColumn(rightDf.columns)
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
