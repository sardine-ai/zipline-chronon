package ai.chronon.orchestration.physical

import ai.chronon.api.DataModel.Entities
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.JoinOps
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.{Join, TableDependency, Window}
import ai.chronon.orchestration.JoinNodeType
import ai.chronon.orchestration.PhysicalNodeType
import ai.chronon.orchestration.utils
import ai.chronon.api.CollectionExtensions.JListExtension
import ai.chronon.orchestration.utils.DependencyResolver.tableDependency
import ai.chronon.orchestration.utils.ShiftConstants.PartitionTimeUnit
import ai.chronon.orchestration.utils.ShiftConstants.noShift
import ai.chronon.orchestration.utils.WindowUtils

class LabelJoin(join: Join) extends TabularNode[Join](join) {

  override def outputTable: String = join.getMetaData.outputLabelTable

  override def nodeType: PhysicalNodeType = utils.PhysicalNodeType.from(JoinNodeType.LABEL_JOIN)

  override def tableDependencies: Seq[TableDependency] = {

    // table dep for left source
    val baseOutputDep = Seq(tableDependency(join.outputAsSource, noShift, noShift))

    // assumes feature_ds is primary partition of label table output
    val labelDeps =
      for (
        parts <- Option(join.labelParts).toSeq;
        part <- parts.labels;
        groupBy = part.groupBy;
        source <- groupBy.sources
      ) yield {
        // quoting comments in the labelJoin code:
        //    offsets are inclusive,
        //    e.g label_ds = 04-03, left_start_offset = left_end_offset = 3,
        //    left_ds will be 04-01
        // source-events: label_ds is between [left_ds + start_offset - max_window, left_ds + end_offset]
        val endOffset = new Window(1 - parts.leftEndOffset, PartitionTimeUnit)
        val startOffset = if (source.dataModel == Entities) {
          new Window(1 - parts.leftStartOffset, PartitionTimeUnit)
        } else {
          val lookBack = WindowUtils.convertUnits(groupBy.maxWindow.get, PartitionTimeUnit).length
          new Window(1 - parts.leftStartOffset + lookBack, PartitionTimeUnit)
        }
        tableDependency(source, startOffset, endOffset)
      }

    baseOutputDep ++ labelDeps

  }

}
