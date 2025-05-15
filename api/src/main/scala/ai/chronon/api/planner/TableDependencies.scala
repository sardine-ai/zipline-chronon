package ai.chronon.api.planner
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.api.{Accuracy, DataModel, PartitionSpec, TableDependency, TableInfo, Window}

object TableDependencies {

  def fromJoin(join: api.Join, labelParts: api.LabelParts)(implicit spec: PartitionSpec): Seq[TableDependency] = {

    val joinParts = labelParts.labels.iterator().toScala.toArray.distinct
    joinParts.flatMap { jp =>
      require(
        jp.groupBy.dataModel == DataModel.EVENTS,
        s"Label GroupBy, ${jp.groupBy.metaData.name}, is not an EventSource. " +
          s"EntitySources are not yet supported on label parts."
      )

      val isTemporal = join.left.dataModel == DataModel.EVENTS && jp.groupBy.inferredAccuracy == Accuracy.TEMPORAL

      val windows: Array[Window] = jp.groupBy.allWindows

      require(
        !windows.contains(null),
        s"All aggregations must be windowed on EventSource labels. " +
          s"Label GroupBy, ${jp.groupBy.metaData.name} has an un-windowed aggregation."
      )

      val minWindow = windows.minBy(_.millis)
      // a 2hr window will need us to scan input partitions from both ds & ds + 1 - latter for events close to midnight
      val maxWindow = (windows :+ WindowUtils.Day).maxBy(_.millis)

      val deps: Seq[TableDependency] =
        if (isTemporal) {
          // depend on source table directly from [ds, ds + maxWindow]
          jp.groupBy.sources
            .iterator()
            .toScala
            .map { source =>
              new TableDependency()
                .setTableInfo(
                  new TableInfo()
                    .setTable(source.table)
                    .setIsCumulative(source.isCumulative)
                    .setPartitionColumn(source.query.getPartitionColumn)
                    .setPartitionFormat(source.query.getPartitionFormat)
                    .setPartitionInterval(source.query.getPartitionInterval)
                )
                .setStartOffset(WindowUtils.zero())
                .setEndOffset(maxWindow.inverse)
                .setStartCutOff(source.query.getStartPartition)
                .setEndCutOff(source.query.getPartitionColumn)
            }
            .toSeq
        } else {

          // snapshots depends on groupBy backfill table from [ds + minWindow, ds + maxWindow]
          Seq(
            new TableDependency()
              .setTableInfo(
                new TableInfo()
                  .setTable(jp.groupBy.metaData.outputTable)
                  .setPartitionColumn("dt")
                  .setPartitionFormat(spec.format)
                  .setPartitionInterval(WindowUtils.hours(spec.spanMillis))
              )
              .setStartOffset(minWindow.inverse)
              .setEndOffset(maxWindow.inverse)
          )

        }

      deps

    }
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

    val startCutOff = source.query.getStartPartition
    val endCutOff = source.query.getEndPartition

    val lagOpt = Option(WindowUtils.plus(source.query.getPartitionLag, shift.orNull))
    val endOffset = lagOpt.orNull

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
