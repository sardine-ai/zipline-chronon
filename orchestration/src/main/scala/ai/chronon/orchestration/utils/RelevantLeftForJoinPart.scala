package ai.chronon.orchestration.utils

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.JoinPartOps
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.orchestration.utils.CollectionExtensions.JMapExtension
import ai.chronon.orchestration.utils.ColumnExpression.getTimeExpression

// TODO(phase-2): This is not wired into the planner yet
// computes subset of the left source that is relevant for a join part
// we cache the join_part table across joins
// we use this logic to compute the join part table

// CAVEAT: changing partition column name will affect output
//        but partition column constant is not part of the conf
case class RelevantLeftForJoinPart(leftTable: String,
                                   leftExpressions: Array[ColumnExpression],
                                   leftWheres: Array[String]) {
  def render: String = {
    val selects = leftExpressions.map(_.render).sorted.mkString(", ")
    val wheres = leftWheres.sorted.map("(" + _ + ")").mkString(" AND ")
    s"SELECT $selects FROM $leftTable WHERE $wheres"
  }
}

object RelevantLeftForJoinPart {

  private def removeNamespace(table: String): String = {
    table.split('.').last.sanitize
  }

  private def nameWithoutTeam(metadata: MetaData): String = {
    metadata.name.split('.').tail.mkString(".")
  }

  def partTableName(join: Join, joinPart: JoinPart): String = {
    val relevantLeft = relevantLeftCompute(join.left, joinPart)
    val rightMetadata = joinPart.groupBy.metaData
    val prefix = Option(joinPart.prefix).map(_.sanitize + "__").getOrElse("")

    // if gb & join are from the same team, we could skip the team name from output table name
    val groupByName = prefix + (if (rightMetadata.team == join.metaData.team) {
                                  nameWithoutTeam(rightMetadata).sanitize
                                } else {
                                  rightMetadata.name.sanitize
                                })

    val combinedHash = HashUtils.md5Hex(relevantLeft.render + joinPart.groupBy.semanticHash).toLowerCase

    // POLICY: caches are computed per team / namespace.
    // we have four options here
    // - use right namespace. other teams typically won't have perms.
    // - use a common cache namespace, but this could a way to leak information outside ACLs
    // - use right input table namespace, also suffers from perm issue.
    // - use the join namespace, this could create duplicate tables, but safest.
    val outputNamespace = join.metaData.outputNamespace

    // removing ns to keep the table name short, hash is enough to differentiate
    val leftTable = removeNamespace(relevantLeft.leftTable)

    s"$outputNamespace.${groupByName}__${leftTable}__$combinedHash"
  }

  // changing the left side shouldn't always change the joinPart table
  // groupBy name + source hash of relevant left side of the groupBy
  private def relevantLeftCompute(left: Source, joinPart: JoinPart): RelevantLeftForJoinPart = {
    val leftQuery = left.query

    // relevant left column computations for the right side
    // (adding new but unrelated selects to left source shouldn't affect these)
    val leftKeyExpressions = joinPart.rightToLeft.map { case (rightKey, leftKey) =>
      ColumnExpression(rightKey, leftQuery.getSelects.safeGet(leftKey))
    }.toArray

    // time is only relevant if left is events
    val leftTimeExpression = left.dataModel match {
      case DataModel.Events => Some(getTimeExpression(leftQuery))
      case _                => None
    }

    val leftExpressions = leftKeyExpressions ++ leftTimeExpression

    // left filter clauses
    val leftFilters: Array[String] = Option(leftQuery.getWheres).iterator.flatMap(_.toScala).toArray

    RelevantLeftForJoinPart(left.table, leftExpressions, leftFilters)
  }

}
