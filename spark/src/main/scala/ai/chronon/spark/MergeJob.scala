package ai.chronon.spark

import ai.chronon.api.DataModel.{Entities, Events}
import ai.chronon.api.Extensions.{
  DateRangeOps,
  DerivationOps,
  ExternalPartOps,
  GroupByOps,
  JoinPartOps,
  MetadataOps,
  SourceOps
}
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.api.{
  Accuracy,
  Constants,
  DateRange,
  JoinPart,
  PartitionRange,
  PartitionSpec,
  QueryUtils,
  RelevantLeftForJoinPart,
  StructField,
  StructType
}
import ai.chronon.online.SparkConversions
import ai.chronon.orchestration.JoinMergeNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils.{coalescedJoin, padFields}
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_add, date_format, monotonically_increasing_id, to_date}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Seq
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/*
leftInputTable is either the output of the SourceJob or the output of the BootstrapJob depending on if there are bootstraps or external parts.

joinPartsToTables is a map of JoinPart to the table name of the output of that joinPart job. JoinParts that are being skipped for this range
due to bootstrap can be omitted from this map.
 */

class MergeJob(node: JoinMergeNode, range: DateRange, joinParts: Seq[JoinPart])(implicit tableUtils: TableUtils) {
  implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val join = node.join
  private val leftSourceTable: String = JoinUtils.computeLeftSourceTableName(join)

  private val leftInputTable = if (join.bootstrapParts != null) {
    join.metaData.bootstrapTable
  } else {
    leftSourceTable
  }

  // Use the node's Join's metadata for output table
  private val outputTable = node.metaData.outputTable
  private val dateRange = range.toPartitionRange

  def run(): Unit = {
    val leftDfForSchema = tableUtils.scanDf(query = null, table = leftInputTable, range = Some(dateRange))
    val leftSchema = leftDfForSchema.schema
    val bootstrapInfo =
      BootstrapInfo.from(join, dateRange, tableUtils, Option(leftSchema), externalPartsAlreadyIncluded = true)

    val requiredColumns: Seq[String] =
      join.joinParts.asScala.flatMap(_.rightToLeft.keys).distinct ++
        Seq(tableUtils.partitionColumn) ++
        (join.left.dataModel match {
          case Entities => Seq.empty
          case Events   => Seq(Constants.TimeColumn, Constants.TimePartitionColumn)
        })

    // This job benefits from a step day of 1 to avoid needing to shuffle on writing output (single partition)
    dateRange.steps(days = 1).foreach { dayStep =>
      logger.info(s"Running merge for ${dayStep.start}")
      val rightPartsData = getRightPartsData(dayStep)

      val leftDf = tableUtils.scanDf(query = null, table = leftInputTable, range = Some(dayStep))

      lazy val leftDfWithUUID = leftDf.withColumn(tableUtils.ziplineInternalRowIdCol, monotonically_increasing_id())

      lazy val requiredColumnsDf =
        leftDfWithUUID.select((requiredColumns ++ Seq(tableUtils.ziplineInternalRowIdCol)).toSeq.map(col): _*)

      val carryOnlyRequiredCols = if (tableUtils.carryOnlyRequiredColsFromLeftInJoin) {
        val result = requiredColumns.toSet != leftDf.schema.fieldNames.toSet
        if (result) {
          logger.info(
            s"Carrying only required columns from left: ${requiredColumns.toSet}"
          )
        } else {
          logger.info(
            s"Carrying all columns from left even though carryOnlyRequiredColsFromLeftInJoin set to true: " +
              s"Required set (${requiredColumns.toSet}) == Entire set (${leftDf.schema.fieldNames.toSet})"
          )
        }
        result
      } else {
        false
      }

      val leftDfForMerge = if (carryOnlyRequiredCols) {
        // If we're only carrying over the required columns from the left we need to
        // take the requiredColumns and the RowId to join back to the non-required values after merge
        requiredColumnsDf
      } else {
        // Else we just need the base DF without the UUID
        leftDf
      }

      val joinedDfTry =
        try {
          Success(
            rightPartsData
              .foldLeft(leftDfForMerge) { case (partialDf, (rightPart, rightDf)) =>
                joinWithLeft(partialDf, rightDf, rightPart)
              }
              // drop all processing metadata columns
              .drop(Constants.MatchedHashes, Constants.TimePartitionColumn))
        } catch {
          case e: Exception =>
            e.printStackTrace()
            Failure(e)
        }

      // leftSource can equal leftDfForMerge columns when there is no bootstrapping
      // We need both schemas for processing and cleaning up contextual fields
      val colsForProcessing = leftDf.columns ++
        (if (carryOnlyRequiredCols) Seq(tableUtils.ziplineInternalRowIdCol) else Seq.empty)
      val mergedDf = processJoinedDf(joinedDfTry, colsForProcessing, bootstrapInfo)

      val outputDf = if (carryOnlyRequiredCols) {
        val nonRequiredColumns =
          leftSchema.fieldNames.filterNot(requiredColumns.contains) ++ Seq(tableUtils.ziplineInternalRowIdCol)
        val nonRequiredDf = leftDfWithUUID.select(nonRequiredColumns.map(col): _*)
        mergedDf
          .join(nonRequiredDf, Array(tableUtils.ziplineInternalRowIdCol), "inner")
          .drop(tableUtils.ziplineInternalRowIdCol)
      } else {
        mergedDf
      }

      outputDf.save(outputTable, node.metaData.tableProps, autoExpand = true)
      logger.info(s"Saved merged data to $outputTable for range ${dayStep.start}")
    }
  }

  private def getRightPartsData(dayStep: PartitionRange): Seq[(JoinPart, DataFrame)] = {
    joinParts.map { joinPart =>
      // Use the RelevantLeftForJoinPart utility to get the part table name
      val partTable = RelevantLeftForJoinPart.fullPartTableName(join, joinPart)
      val effectiveRange =
        if (join.left.dataModel != Entities && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
          dayStep.shift(-1)
        } else {
          dayStep
        }
      val wheres = effectiveRange.whereClauses(tableUtils.partitionColumn)
      val sql = QueryUtils.build(null, partTable, wheres)
      logger.info(s"Pulling data from joinPart table with: $sql")
      (joinPart, tableUtils.scanDfBase(null, partTable, List.empty, wheres, None))
    }.toSeq
  }

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray

    // compute join keys, besides the groupBy keys -  like ds, ts etc.,
    val additionalKeys: Seq[String] = {
      if (join.left.dataModel == Entities) {
        Seq(tableUtils.partitionColumn)
      } else if (joinPart.groupBy.inferredAccuracy == Accuracy.TEMPORAL) {
        Seq(Constants.TimeColumn, tableUtils.partitionColumn)
      } else { // left-events + snapshot => join-key = ds_of_left_ts
        Seq(Constants.TimePartitionColumn)
      }
    }
    val keys = partLeftKeys ++ additionalKeys

    // apply prefix to value columns
    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
                                                                     tableUtils.partitionColumn,
                                                                     Constants.TimePartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)
    val prefixedRightDf = rightDf.prefixColumnNames(joinPart.fullPrefix, valueColumns)

    // apply key-renaming to key columns
    val newColumns = prefixedRightDf.columns.map { column =>
      if (joinPart.rightToLeft.contains(column)) {
        col(column).as(joinPart.rightToLeft(column))
      } else {
        col(column)
      }
    }

    val keyRenamedRightDf = prefixedRightDf.select(newColumns: _*)

    // adjust join keys
    val joinableRightDf = if (additionalKeys.contains(Constants.TimePartitionColumn)) {
      // increment one day to align with left side ts_ds
      // because one day was decremented from the partition range for snapshot accuracy
      keyRenamedRightDf
        .withColumn(
          Constants.TimePartitionColumn,
          date_format(date_add(to_date(col(tableUtils.partitionColumn), tableUtils.partitionSpec.format), 1),
                      tableUtils.partitionSpec.format)
        )
        .drop(tableUtils.partitionColumn)
    } else {
      keyRenamedRightDf
    }

    logger.info(s"""
                   |Join keys for ${joinPart.groupBy.metaData.name}: ${keys.mkString(", ")}
                   |Left Schema:
                   |${leftDf.schema.pretty}
                   |Right Schema:
                   |${joinableRightDf.schema.pretty}""".stripMargin)
    val joinedDf = coalescedJoin(leftDf, joinableRightDf, keys)
    logger.info(s"""Final Schema:
                   |${joinedDf.schema.pretty}
                   |""".stripMargin)

    joinedDf
  }

  private def toSparkSchema(fields: Seq[StructField]): sql.types.StructType =
    SparkConversions.fromChrononSchema(StructType("", fields.toArray))

  /*
   * For all external fields that are not already populated during the group by backfill step, fill in NULL.
   * This is so that if any derivations depend on the these group by fields, they will still pass and not complain
   * about missing columns. This is necessary when we directly bootstrap a derived column and skip the base columns.
   */
  private def padGroupByFields(baseJoinDf: DataFrame, bootstrapInfo: BootstrapInfo, allBaseCols: Seq[String]): DataFrame = {
    // if we're not carrying heavy fields, the baseJoinDf might be missing some columns that we'll join back
    // later in the job, in that case we don't want to pad them here, that's the `allBaseCols` argument
    val groupByFields = toSparkSchema(bootstrapInfo.joinParts.flatMap(_.valueSchema)
      .filterNot(field => allBaseCols.contains(field.name))
    )
    padFields(baseJoinDf, groupByFields)
  }

  def cleanUpContextualFields(finalDf: DataFrame, bootstrapInfo: BootstrapInfo, leftColumns: Seq[String]): DataFrame = {

    val contextualNames =
      bootstrapInfo.externalParts.filter(_.externalPart.isContextual).flatMap(_.keySchema).map(_.name)
    val projections = if (join.isSetDerivations) {
      join.derivations.toScala.derivationProjection(bootstrapInfo.baseValueNames).map(_._1)
    } else {
      Seq()
    }
    contextualNames.foldLeft(finalDf) { case (df, name) =>
      if (leftColumns.contains(name) || projections.contains(name)) {
        df
      } else {
        df.drop(name)
      }
    }
  }

  def processJoinedDf(joinedDfTry: Try[DataFrame],
                      leftCols: Seq[String],
                      bootstrapInfo: BootstrapInfo): DataFrame = {
    if (joinedDfTry.isFailure) throw joinedDfTry.failed.get
    val joinedDf = joinedDfTry.get
    val outputColumns = joinedDf.columns.filter(bootstrapInfo.fieldNames ++ leftCols)
    val finalBaseDf = padGroupByFields(joinedDf.selectExpr(outputColumns.map(c => s"`$c`"): _*), bootstrapInfo, leftCols)
    val finalDf = cleanUpContextualFields(finalBaseDf, bootstrapInfo, leftCols)
    finalDf.explain()
    finalDf
  }

}
