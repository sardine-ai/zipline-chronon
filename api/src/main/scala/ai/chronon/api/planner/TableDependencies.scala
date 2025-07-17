package ai.chronon.api.planner
import ai.chronon.api
import ai.chronon.api.{Accuracy, BootstrapPart, DataModel, TableDependency, TableInfo, Window}
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.{IterableOps, IteratorOps}

import scala.collection.JavaConverters._

object TableDependencies {

  def fromStagingQuery(stagingQuery: api.StagingQuery): Seq[TableDependency] = {
    Option(stagingQuery.tableDependencies)
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)
  }

  def fromJoin(join: api.Join): Seq[TableDependency] = {
    val joinParts = Option(join.joinParts).map(_.iterator().toScala.toArray).getOrElse(Array.empty)
    val joinPartDeps = joinParts.flatMap((jp) => fromGroupBy(jp.groupBy))
    val leftDep = fromSource(join.left)
    val bootstrap =
      scala.Option(join.bootstrapParts).map(_.toScala.toArray[BootstrapPart]).getOrElse(Array.empty[BootstrapPart])
    val bootstrapDeps = bootstrap.map((bp) => fromTable(bp.table, bp.query))
    (leftDep.toSeq ++ joinPartDeps.toSeq ++ bootstrapDeps.toSeq)
  }

  def fromGroupBy(groupBy: api.GroupBy, leftDataModel: Option[DataModel] = None): Seq[TableDependency] =
    groupBy.sources
      .iterator()
      .toScala
      .flatMap { source =>
        val lookback = if (source.dataModel == DataModel.EVENTS && !source.isCumulative) groupBy.maxWindow else None

        def dep(shift: Option[Window] = None, forMutations: Boolean = false): Option[TableDependency] =
          TableDependencies.fromSource(source, lookback, shift)

        (leftDataModel, groupBy.inferredAccuracy, source.dataModel) match {

          case (Some(api.DataModel.EVENTS), Accuracy.TEMPORAL, DataModel.ENTITIES) =>
            dep(shift = Some(source.partitionInterval)) ++ dep(forMutations = true)

          case (Some(api.DataModel.EVENTS), Accuracy.SNAPSHOT, _) => dep(shift = Some(source.partitionInterval))

          case _ => dep()

        }
      }
      .toSeq

  def fromSource(source: api.Source,
                 maxWindowOpt: Option[Window] = None,
                 shift: Option[Window] = None,
                 forMutations: Boolean = false): Option[TableDependency] = {

    if (forMutations && source.mutationsTable.isEmpty) return None

    val startCutOff = Option(source.query).map(_.getStartPartition).orNull
    val endCutOff = Option(source.query).map(_.getEndPartition).orNull

    val lagOpt = Option(WindowUtils.plus(source.query.getPartitionLag, shift.orNull))
    val endOffset = lagOpt.getOrElse(WindowUtils.zero())

    // we don't care if the source is cumulative YET.
    // Downstream partitionRange calculation logic will need to look at tableInfo and use that
    // to resolve the dependency if any of the partition on or after the endOffset is present.
    // In the scheduler we can kick off steps whose end_date - offset < latest_available_partition.
    val startOffset: Window = (source.dataModel, maxWindowOpt, lagOpt) match {
      case (DataModel.ENTITIES, _, _) => endOffset
      // when start offset is null, we won't try to fill anything
      // we go by the amount of data that is available in the source.
      case (DataModel.EVENTS, None, _) => null

      case (DataModel.EVENTS, Some(aggregationWindow), _) =>
        lagOpt match {
          case Some(lag) => WindowUtils.plus(aggregationWindow, lag)
          case None      => aggregationWindow
        }
    }

    val inputTable = if (forMutations) source.mutationsTable.get else source.rawTable

    val tableDep = new TableDependency()
      .setTableInfo(
        new TableInfo()
          .setTable(inputTable)
          .setIsCumulative(source.isCumulative)
          .setPartitionColumn(source.query.getPartitionColumn)
          .setPartitionFormat(source.query.getPartitionFormat)
          .setPartitionInterval(source.query.getPartitionInterval)
      )
      .setStartOffset(startOffset)
      .setEndOffset(endOffset)
      .setStartCutOff(startCutOff)
      .setEndCutOff(endCutOff)

    Some(tableDep)
  }

  // label join modifies the table inplace. sensing for existing partitions won't tell us if label join for those dates
  // has already run. We need to keep track of it separately - maybe via table properties.
  def fromTable(table: String, query: api.Query = null, shift: Option[Window] = None): TableDependency = {

    if (query == null)
      return new TableDependency()
        .setTableInfo(
          new TableInfo()
            .setTable(table)
        )
        .setStartOffset(shift.getOrElse(WindowUtils.zero()))
        .setEndOffset(shift.getOrElse(WindowUtils.zero()))

    val offset = Option(query.partitionLag).orElse(shift).getOrElse(WindowUtils.zero())

    new TableDependency()
      .setTableInfo(
        new TableInfo()
          .setTable(table)
          .setPartitionColumn(query.getPartitionColumn)
          .setPartitionFormat(query.getPartitionFormat)
          .setPartitionInterval(query.getPartitionInterval)
      )
      .setStartOffset(offset)
      .setEndOffset(offset)
      .setStartCutOff(query.startPartition)
      .setEndCutOff(query.endPartition)

  }

}
