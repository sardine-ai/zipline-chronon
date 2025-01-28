package ai.chronon.orchestration.physical

import ai.chronon.api.Accuracy
import ai.chronon.api.DataModel.Entities
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.Join
import ai.chronon.api.Window
import ai.chronon.orchestration.JoinNodeType
import ai.chronon.orchestration.PhysicalNodeType
import ai.chronon.orchestration.Table
import ai.chronon.orchestration.TableDependency
import ai.chronon.orchestration.utils
import ai.chronon.orchestration.utils.CollectionExtensions.JListExtension
import ai.chronon.orchestration.utils.DependencyResolver.add
import ai.chronon.orchestration.utils.DependencyResolver.tableDependency
import ai.chronon.orchestration.utils.ShiftConstants.noShift
import ai.chronon.orchestration.utils.ShiftConstants.shiftOne

class JoinBackfill(join: Join) extends TabularNode[Join](join) {

  override def outputTable: String = join.getMetaData.outputTable

  override def nodeType: PhysicalNodeType = utils.PhysicalNodeType.from(JoinNodeType.BACKFILL)

  override def tableDependencies: Seq[TableDependency] = {
    // left source
    val leftDep = Seq(tableDependency(join.left, noShift, noShift))

    // bootstrap sources
    val bootstrapDeps = join.bootstrapParts.map { bootstrapPart =>
      val query = bootstrapPart.getQuery

      val dep = new TableDependency()
      dep.setStartOffset(noShift)
      dep.setEndOffset(noShift)
      dep.setStartCutOff(query.getStartPartition)
      dep.setEndCutOff(query.getEndPartition)
      dep.setIsCumulative(false)
      dep.setTable(new Table().setTable(bootstrapPart.getTable))

      dep
    }

    val leftModel = join.left.dataModel

    // right sources
    val rightTableLists =
      for (
        part <- join.joinParts;
        gb = part.groupBy; lookBack = gb.maxWindow;
        source <- gb.sources
      ) yield {

        def depBuilder(startOffset: Window, endOffset: Window, isMutation: Boolean = false): TableDependency =
          tableDependency(source, startOffset, endOffset, isMutation)

        (leftModel, source.dataModel, gb.inferredAccuracy, source.isCumulative) match {

          case (Events, Entities, Accuracy.SNAPSHOT, false) => Seq(depBuilder(shiftOne, shiftOne))

          case (Events, Entities, Accuracy.TEMPORAL, false) =>
            Seq(depBuilder(shiftOne, shiftOne)) ++ Seq(depBuilder(noShift, noShift, isMutation = true))

          case (Events, Events, Accuracy.SNAPSHOT, false) =>
            Seq(depBuilder(shiftOne, lookBack.map(add(_, shiftOne)).orNull))

          case (_, Events, _, false) => Seq(depBuilder(noShift, lookBack.orNull))

          // all cumulative cases and all non-events left entities cases
          case _ => Seq(depBuilder(noShift, noShift))
        }

      }

    leftDep ++ rightTableLists.flatten.toSeq ++ bootstrapDeps
  }
}
