package ai.chronon.api.test.planner

import ai.chronon.api.planner.{LocalRunner, MonolithJoinPlanner}
import ai.chronon.api
import ai.chronon.api.Builders.{Join, MetaData}
import ai.chronon.api.{ExecutionInfo, PartitionSpec}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

class MonolithJoinPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec = PartitionSpec.daily

  it should "monolith join planner plans valid confs without exceptions" in {

    val runfilesDir = System.getenv("RUNFILES_DIR")
    val rootDir = Paths.get(runfilesDir, "chronon/spark/src/test/resources/canary/compiled/joins")

    val joinConfs = LocalRunner.parseConfs[api.Join](rootDir.toString)

    val joinPlanners = joinConfs.map(MonolithJoinPlanner(_))

    joinPlanners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          plan.terminalNodeNames.asScala.size should be > 0
        }
      }
  }

  it should "monolith join should avoid metadata when computing semantic hash" in {
    val firstJoin = Join(
      metaData = MetaData(name = "firstJoin", executionInfo = new ExecutionInfo().setStepDays(2)),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val secondJoin = Join(
      metaData = MetaData(name = "secondJoin", executionInfo = new ExecutionInfo().setStepDays(1)),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )
    val firstPlan = MonolithJoinPlanner(firstJoin).buildPlan
    val secondPlan = MonolithJoinPlanner(secondJoin).buildPlan

    // Semantic hashes should be identical since metadata is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "monolith join planner should create valid terminal node names" in {
    val join = Join(
      metaData = MetaData(name = "testJoin"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.BACKFILL
    plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.BACKFILL) should equal("testJoin/backfill")
  }

  it should "monolith join planner should respect step days from execution info" in {
    val joinWithStepDays = Join(
      metaData = MetaData(name = "testJoin", executionInfo = new ExecutionInfo().setStepDays(5)),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val joinWithoutStepDays = Join(
      metaData = MetaData(name = "testJoin2"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val plannerWithStepDays = MonolithJoinPlanner(joinWithStepDays)
    val plannerWithoutStepDays = MonolithJoinPlanner(joinWithoutStepDays)

    noException should be thrownBy {
      plannerWithStepDays.buildPlan
      plannerWithoutStepDays.buildPlan
    }
  }

  it should "monolith join planner should produce same semantic hash with different executionInfo" in {
    val joinWithExecutionInfo1 = Join(
      metaData = MetaData(
        name = "testJoin1",
        executionInfo = new ExecutionInfo().setStepDays(3)
      ),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val joinWithExecutionInfo2 = Join(
      metaData = MetaData(
        name = "testJoin2",
        executionInfo = new ExecutionInfo().setStepDays(7)
      ),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val firstPlan = MonolithJoinPlanner(joinWithExecutionInfo1).buildPlan
    val secondPlan = MonolithJoinPlanner(joinWithExecutionInfo2).buildPlan

    // Semantic hashes should be identical since executionInfo is part of metadata and excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "monolith join planner should produce exactly one node wrapping a Join for canary confs" in {
    val runfilesDir = System.getenv("RUNFILES_DIR")
    val rootDir = Paths.get(runfilesDir, "chronon/spark/src/test/resources/canary/compiled/joins")

    val joinConfs = LocalRunner.parseConfs[api.Join](rootDir.toString)

    joinConfs.foreach { joinConf =>
      val planner = MonolithJoinPlanner(joinConf)
      val plan = planner.buildPlan

      // Should have exactly one node
      plan.nodes.asScala should have size 1

      val node = plan.nodes.asScala.head
      // Node should have content
      node.content should not be null
      // Content should have a monolithJoin set
      node.content.getMonolithJoin should not be null
      // The wrapped join should not be null
      node.content.getMonolithJoin.join should not be null
    }
  }
}
