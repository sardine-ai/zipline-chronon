package ai.chronon.spark

import ai.chronon.api.DataModel.Entities
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
import org.apache.spark.sql.functions.{col, date_add, date_format, to_date}
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
  private val leftInputTable = join.metaData.bootstrapTable
  // Use the node's Join's metadata for output table
  private val outputTable = node.metaData.outputTable
  private val dateRange = range.toPartitionRange

  def run(): Unit = {
    val leftDf = tableUtils.scanDf(query = null, table = leftInputTable, range = Some(dateRange))
    val leftSchema = leftDf.schema
    val bootstrapInfo =
      BootstrapInfo.from(join, dateRange, tableUtils, Option(leftSchema), externalPartsAlreadyIncluded = true)

    val rightPartsData = getRightPartsData()

    val joinedDfTry =
      try {
        Success(
          rightPartsData
            .foldLeft(leftDf) { case (partialDf, (rightPart, rightDf)) =>
              joinWithLeft(partialDf, rightDf, rightPart)
            }
            // drop all processing metadata columns
            .drop(Constants.MatchedHashes, Constants.TimePartitionColumn))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Failure(e)
      }
    val df = processJoinedDf(joinedDfTry, leftDf, bootstrapInfo, leftDf)
    df.save(outputTable, node.metaData.tableProps, autoExpand = true)
  }

  private def getRightPartsData(): Seq[(JoinPart, DataFrame)] = {
    joinParts.map { joinPart =>
      // Use the RelevantLeftForJoinPart utility to get the part table name
      val partTable = RelevantLeftForJoinPart.fullPartTableName(join, joinPart)
      val effectiveRange =
        if (join.left.dataModel != Entities && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
          dateRange.shift(-1)
        } else {
          dateRange
        }
      val wheres = effectiveRange.whereClauses("ds")
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
  private def padGroupByFields(baseJoinDf: DataFrame, bootstrapInfo: BootstrapInfo): DataFrame = {
    val groupByFields = toSparkSchema(bootstrapInfo.joinParts.flatMap(_.valueSchema))
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
                      leftDf: DataFrame,
                      bootstrapInfo: BootstrapInfo,
                      bootstrapDf: DataFrame): DataFrame = {
    if (joinedDfTry.isFailure) throw joinedDfTry.failed.get
    val joinedDf = joinedDfTry.get
    val outputColumns = joinedDf.columns.filter(bootstrapInfo.fieldNames ++ bootstrapDf.columns)
    val finalBaseDf = padGroupByFields(joinedDf.selectExpr(outputColumns.map(c => s"`$c`"): _*), bootstrapInfo)
    val finalDf = cleanUpContextualFields(finalBaseDf, bootstrapInfo, leftDf.columns)
    finalDf.explain()
    finalDf
  }

}
