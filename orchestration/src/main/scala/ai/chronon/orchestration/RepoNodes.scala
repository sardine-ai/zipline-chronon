package ai.chronon.orchestration

import ai.chronon.api._
import ai.chronon.orchestration.logical._

// Main LogicalSet class
case class RepoNodes(joins: Seq[Join] = Seq.empty,
                     groupBys: Seq[GroupBy] = Seq.empty,
                     stagingQueries: Seq[StagingQuery] = Seq.empty,
                     models: Seq[Model] = Seq.empty) {

  def toLogicalNodes: Seq[LogicalNodeImpl] =
    joins.map(JoinNodeImpl) ++
      groupBys.map(GroupByNodeImpl) ++
      stagingQueries.map(StagingQueryNodeImpl) ++
      models.map(ModelNodeImpl)

  def :+(join: Join): RepoNodes = copy(joins = joins :+ join)

  def :+(groupBy: GroupBy): RepoNodes = copy(groupBys = groupBys :+ groupBy)

  def :+(stagingQuery: StagingQuery): RepoNodes = copy(stagingQueries = stagingQueries :+ stagingQuery)

  def :+(model: Model): RepoNodes = copy(models = models :+ model)
}
