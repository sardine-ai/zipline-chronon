package ai.chronon.api.test.planner

import ai.chronon.api.planner.{LocalRunner, MonolithJoinPlanner}
import ai.chronon.api
import ai.chronon.api.Builders.{Join, MetaData}
import ai.chronon.api.{ExecutionInfo, PartitionSpec}
import ai.chronon.planner.{ConfPlan, Mode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.jdk.CollectionConverters._
import ai.chronon.api.Builders

class MonolithJoinPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def validateJoinPlan(plan: ConfPlan): Unit = {
    // Should create plan successfully with both backfill and metadata upload nodes
    plan.nodes.asScala should have size 2

    // Find the backfill node and metadata upload node
    val backfillNode = plan.nodes.asScala.find(_.content.isSetMonolithJoin)
    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload)

    backfillNode should be(defined)
    metadataUploadNode should be(defined)

    // Backfill node should have content
    backfillNode.get.content should not be null
    backfillNode.get.content.getMonolithJoin should not be null
    backfillNode.get.content.getMonolithJoin.join should not be null

    // Metadata upload node should have content
    metadataUploadNode.get.content should not be null
    metadataUploadNode.get.content.getJoinMetadataUpload should not be null
    metadataUploadNode.get.content.getJoinMetadataUpload.join should not be null

    plan.terminalNodeNames.asScala.size shouldBe 2
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) shouldBe true
    plan.terminalNodeNames.containsKey(Mode.BACKFILL) shouldBe true
  }

  it should "monolith join planner plans valid confs without exceptions" in {

    val runfilesDir = System.getenv("RUNFILES_DIR")
    val rootDir = Paths.get(runfilesDir, "chronon/spark/src/test/resources/canary/compiled/joins")

    val joinConfs = LocalRunner.parseConfs[api.Join](rootDir.toString)

    val joinPlanners = joinConfs.map(MonolithJoinPlanner(_))

    joinPlanners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          validateJoinPlan(plan)
        }
      }
  }

  it should "monolith join should avoid metadata when computing semantic hash" in {
    val firstJoin = Join(
      metaData = MetaData(name = "firstJoin", executionInfo = new ExecutionInfo().setStepDays(2)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val secondJoin = Join(
      metaData = MetaData(name = "secondJoin", executionInfo = new ExecutionInfo().setStepDays(1)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_table"),
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
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.BACKFILL
    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.DEPLOY
    plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.BACKFILL) should equal("testJoin/backfill")
    plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.DEPLOY) should equal("testJoin/metadata_upload")
  }

  it should "monolith join planner should respect step days from execution info" in {
    val joinWithStepDays = Join(
      metaData = MetaData(name = "testJoin", executionInfo = new ExecutionInfo().setStepDays(5)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_with_step_days_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val joinWithoutStepDays = Join(
      metaData = MetaData(name = "testJoin2"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_without_step_days_table"),
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

  it should "set nonzero step days" in {
    val joinWithNonZeroStepDays = Join(
      metaData = MetaData(name = "testJoin", executionInfo = new ExecutionInfo()),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_non_zero_step_days_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val plannerWithNonZeroStepDays = MonolithJoinPlanner(joinWithNonZeroStepDays)
    val plan = plannerWithNonZeroStepDays.buildPlan
    plan.nodes.asScala.foreach((node) => node.metaData.executionInfo.stepDays should equal(1))
  }

  it should "monolith join planner should produce same semantic hash with different executionInfo" in {
    val joinWithExecutionInfo1 = Join(
      metaData = MetaData(
        name = "testJoin1",
        executionInfo = new ExecutionInfo().setStepDays(3)
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_execution_info_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val joinWithExecutionInfo2 = Join(
      metaData = MetaData(
        name = "testJoin2",
        executionInfo = new ExecutionInfo().setStepDays(7)
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_execution_info_table"),
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

  it should "monolith join planner should produce exactly two nodes (backfill and metadata upload) for canary confs" in {
    val runfilesDir = System.getenv("RUNFILES_DIR")
    val rootDir = Paths.get(runfilesDir, "chronon/spark/src/test/resources/canary/compiled/joins")

    val joinConfs = LocalRunner.parseConfs[api.Join](rootDir.toString)

    joinConfs.foreach { joinConf =>
      val planner = MonolithJoinPlanner(joinConf)
      val plan = planner.buildPlan

      validateJoinPlan(plan)
    }
  }

  it should "monolith join planner should create metadata upload node with correct properties" in {
    val join = Join(
      metaData = MetaData(name = "testJoin"),
      left = Builders.Source.events(Builders.Query(), table = s"test_namespace.test_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Verify metadata upload node name
    metadataUploadNode.metaData.name should equal("testJoin/metadata_upload")

    // Verify the wrapped join is correct
    metadataUploadNode.content.getJoinMetadataUpload.join should equal(join)
  }

  it should "monolith join planner should skip metadata in semantic hash for both nodes" in {
    val firstJoin = Join(
      metaData = MetaData(name = "firstJoin", executionInfo = new ExecutionInfo().setStepDays(2)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_semantic_hash_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val secondJoin = Join(
      metaData = MetaData(name = "secondJoin", executionInfo = new ExecutionInfo().setStepDays(1)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_semantic_hash_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val firstPlan = MonolithJoinPlanner(firstJoin).buildPlan
    val secondPlan = MonolithJoinPlanner(secondJoin).buildPlan

    // Semantic hashes should be identical for both backfill and metadata upload nodes
    val firstBackfillHash = firstPlan.nodes.asScala.find(_.content.isSetMonolithJoin).get.semanticHash
    val secondBackfillHash = secondPlan.nodes.asScala.find(_.content.isSetMonolithJoin).get.semanticHash
    firstBackfillHash should equal(secondBackfillHash)

    val firstMetadataUploadHash = firstPlan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get.semanticHash
    val secondMetadataUploadHash = secondPlan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get.semanticHash
    firstMetadataUploadHash should equal(secondMetadataUploadHash)
  }
}
