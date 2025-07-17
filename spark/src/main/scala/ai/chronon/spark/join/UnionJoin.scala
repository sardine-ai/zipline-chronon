package ai.chronon.spark.join

import ai.chronon.api
import ai.chronon.api.Extensions.{DerivationOps, GroupByOps, JoinPartOps, MetadataOps}
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.api.{Accuracy, Constants, PartitionRange}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, functions => F}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object UnionJoin {

  private val logger = LoggerFactory.getLogger(this.getClass)

  case class UnionDf(df: DataFrame, leftStruct: StructType, rightStruct: StructType)

  /** Unions left and right dataframes with different schemas by normalizing then
    * and creates time-sorted arrays of structs of non-key-columns.
    */
  def unionJoin(left: DataFrame,
                right: DataFrame,
                leftKeyToRightKeyMap: Map[String, String],
                leftTimeCol: String,
                rightTimeCol: String): UnionDf = {

    val leftToRight = leftKeyToRightKeyMap.toSeq
    val leftKeyCols = leftToRight.map(_._1)
    val rightKeyCols = leftToRight.map(_._2)

    // time != null AND one of the keys is non-null
    val filteredLeft = left
      .filter(F.col(leftTimeCol).isNotNull)
      .filter(
        leftKeyCols
          .map(F.col(_).isNotNull)
          .reduce(_.or(_)))

    // time != null AND one of the keys is non-null
    val filteredRight = right
      .filter(F.col(rightTimeCol).isNotNull)
      .filter(
        rightKeyCols
          .map(F.col(_).isNotNull)
          .reduce(_.or(_)))

    val leftDataCols = filteredLeft.columns.filterNot(leftKeyCols.contains)
    val rightDataCols = filteredRight.columns.filterNot(rightKeyCols.contains)

    // Extract the struct types
    val leftStructType = StructType(leftDataCols.map { col => left.schema.find(_.name == col).get })
    val rightStructType = StructType(rightDataCols.map { col => right.schema.find(_.name == col).get })

    val leftSelected = filteredLeft.select(
      // keys
      leftKeyCols.map(k => F.col(k).as(k)) :+
        // left_data struct
        F.struct(
          leftDataCols.map(c => F.col(c).as(c)): _*
        ).as("left_data") :+
        // null as right_data
        F.lit(null).cast(rightStructType).as("right_data"): _*
    )

    val rightSelected = filteredRight.select(
      // keys
      rightKeyCols.zip(leftKeyCols).map { case (rightKey, leftKey) => F.col(rightKey).as(leftKey) } :+
        // null as left_data
        F.lit(null).cast(leftStructType).as("left_data") :+
        // right_data struct
        F.struct(
          (rightDataCols).map(c => F.col(c).as(c)): _*
        ).as("right_data"): _*
    )

    val unioned = leftSelected.union(rightSelected)

    val result = unioned
      .groupBy(leftKeyCols.map(F.col): _*)
      .agg(
        F.collect_list("left_data").as("left_data_array_unsorted"),
        F.collect_list("right_data").as("right_data_array_unsorted")
      )
      // filter out where there is no left_data (left_outer_join)
      .filter("left_data_array_unsorted IS NOT NULL")
      // sort by timestamp
      .select(
        leftKeyCols.map(F.col) :+
          F.expr(s"""
        array_sort(
          left_data_array_unsorted,
          (left, right) -> case when left.$leftTimeCol < right.$leftTimeCol then -1 when left.$leftTimeCol > right.$leftTimeCol then 1 else 0 end
        )
      """).as("left_data_array") :+
          F.expr(s"""
        array_sort(
          right_data_array_unsorted,
          (left, right) -> case when left.$rightTimeCol < right.$rightTimeCol then -1 when left.$rightTimeCol > right.$rightTimeCol then 1 else 0 end
        )
      """).as("right_data_array"): _*
      )

    UnionDf(result, leftStructType, rightStructType)
  }

  def computeJoinPart(leftDf: DataFrame,
                      joinPart: api.JoinPart,
                      dateRange: PartitionRange,
                      produceFinalJoinOutput: Boolean)(implicit tableUtils: TableUtils): DataFrame = {

    val disclaimer = "Support is landing soon."
    val groupBy = joinPart.groupBy

    require(groupBy.inferredAccuracy == Accuracy.TEMPORAL,
            s"Only realtime accurate GroupBy's are supported for now. $disclaimer")

    // Select only relevant columns early when not including all left columns
    val selectedLeftDf = if (produceFinalJoinOutput) {
      leftDf
    } else {
      val keyColumns = joinPart.leftToRight.keys.toSeq :+ Constants.TimeColumn :+ tableUtils.partitionColumn
      val existingColumns = leftDf.columns.toSet
      val columnsToSelect = keyColumns.filter(existingColumns.contains)
      leftDf.select(columnsToSelect.map(F.col): _*).dropDuplicates(keyColumns)
    }

    val rightDf = ai.chronon.spark.GroupBy.inputDf(groupBy, dateRange, tableUtils)

    val unionDf = unionJoin(
      selectedLeftDf,
      rightDf,
      joinPart.leftToRight,
      leftTimeCol = Constants.TimeColumn,
      rightTimeCol = Constants.TimeColumn
    )

    val minQueryTs = dateRange.partitionSpec.epochMillis(dateRange.start)
    val aggregationInfo = AggregationInfo.from(groupBy, minQueryTs, unionDf.leftStruct, unionDf.rightStruct)

    def indexOf(column: String): Int = unionDf.df.schema.indexWhere(_.name == column)
    val leftIdx = indexOf("left_data_array")
    val rightIdx = indexOf("right_data_array")
    val leftKeys = joinPart.leftToRight.keys.toArray
    val leftKeyIndices = leftKeys.map(indexOf)

    val outputSchema: StructType = StructType(
      (leftKeys.map(k => unionDf.df.schema.find(_.name == k).get) ++
        aggregationInfo.outputSparkSchema)
    )

    // when I tried to do this via udf + dataframe, spark simply OOMs
    // analyzing with profiler indicates that the catalyst generated code creates
    // too much garbage when dealing with UDF outputs
    // and it particularly memory intensive.
    val outputRdd: RDD[Row] = unionDf.df.rdd.flatMap { row: Row =>
      val leftData = row.get(leftIdx).asInstanceOf[mutable.WrappedArray[Row]]
      val rightData = row.get(rightIdx).asInstanceOf[mutable.WrappedArray[Row]]
      val aggregatedData: Array[CGenericRow] = aggregationInfo.aggregate(leftData, rightData)
      val keys = leftKeyIndices.map(row.get)

      aggregatedData.iterator.map { data =>
        val values = data.values
        val result = new Array[Any](keys.length + values.length)

        System.arraycopy(keys, 0, result, 0, keys.length)
        System.arraycopy(values, 0, result, keys.length, values.length)

        new GenericRow(result)
      }
    }

    val baseResultDf = tableUtils.sparkSession.createDataFrame(outputRdd, outputSchema)

    // Apply GroupBy derivations to the aggregated results if we're producing final join output
    // Else, it gets applied later in the JoinPartJob
    if (groupBy.hasDerivations && produceFinalJoinOutput) {
      // In the case that we're producing final join output correctly, we don't want the derivations to erase base
      // left DF columns. So we leverage the `ensureKeys` functionality on finalOutputCols to ensure that they are preserved
      val finalOutputColumns =
        groupBy.derivationsScala.finalOutputColumn(baseResultDf.columns, ensureKeys = leftDf.columns.toSeq)
      baseResultDf.select(finalOutputColumns: _*)
    } else {
      baseResultDf
    }
  }

  def computeJoin(joinConf: api.Join, dateRange: PartitionRange, includeAllLeftColumns: Boolean = true)(implicit
      tableUtils: TableUtils): DataFrame = {

    val disclaimer = "Support is coming soon."
    require(joinConf.left.isSetEvents, s"Only events sources are supported on the left side of the join. $disclaimer")
    require(!joinConf.isSetBootstrapParts, s"Bootstraps on fast mode are not supported yet. $disclaimer")
    require(!joinConf.isSetLabelParts, s"Label Parts on fast mode are not supported yet. $disclaimer")

    // For multi-join-part case, only validate single join part when includeAllLeftColumns is true
    if (includeAllLeftColumns) {
      require(joinConf.getJoinParts.size() == 1, s"Only one join-part is supported on fast mode. $disclaimer")
    }

    val joinPart = joinConf.getJoinParts.get(0)
    val leftDf = JoinUtils.leftDf(joinConf, dateRange, tableUtils).get

    val groupByDerivedDf = computeJoinPart(leftDf, joinPart, dateRange, includeAllLeftColumns)

    // Apply Join derivations if they exist
    if (joinConf.isSetDerivations && !joinConf.derivations.isEmpty()) {
      val derivations = joinConf.derivations.toScala
      val finalOutputColumns = derivations.finalOutputColumn(groupByDerivedDf.columns)
      groupByDerivedDf.select(finalOutputColumns: _*)
    } else {
      groupByDerivedDf
    }
  }

  def computeJoinAndSave(joinConf: api.Join, dateRange: PartitionRange)(implicit tableUtils: TableUtils): Unit = {
    val resultDf = computeJoin(joinConf, dateRange, includeAllLeftColumns = true)
    logger.info(s"Saving output to ${joinConf.metaData.outputTable}")
    resultDf.save(joinConf.metaData.outputTable)
  }

}
