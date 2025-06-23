package ai.chronon.api.test.planner

import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.planner.{GroupByUploadPlanner, LocalRunner}
import ai.chronon.api.test.planner.GroupByUploadPlannerTest.buildGroupBy
import ai.chronon.api.{Accuracy, Aggregation, Builders, EnvironmentVariables, GroupBy, Operation, PartitionSpec}
import ai.chronon.planner.{ConfPlan, Mode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

class GroupByUploadPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def validateGBPlan(plan: ConfPlan): Unit = {
    // Should create plan successfully with default step days
    plan.nodes.asScala should have size 1

    // Node should have content
    val node = plan.nodes.asScala.head
    node.content should not be null

    // Content should have a groupBy set
    node.content.getGroupByUpload should not be null
    // The wrapped groupBy should not be null
    node.content.getGroupByUpload.groupBy should not be null

    plan.terminalNodeNames.asScala.size should be > 0
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) shouldBe true
  }

  it should "GBU planner handles valid confs" in {

    val runfilesDir = System.getenv("RUNFILES_DIR")
    val gbRootDir = Paths.get(runfilesDir, "chronon/spark/src/test/resources/canary/compiled/group_bys")

    val gbConfs = LocalRunner.parseConfs[ai.chronon.api.GroupBy](gbRootDir.toString)

    val planners = gbConfs.map(GroupByUploadPlanner(_))

    planners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          validateGBPlan(plan)
        }
      }
  }

  it should "GBU planner should create valid plans" in {
    val gb = buildGroupBy()

    val planner = GroupByUploadPlanner(gb)

    noException should be thrownBy {
      val plan = planner.buildPlan
      validateGBPlan(plan)
      plan.terminalNodeNames.asScala(Mode.DEPLOY) should equal("user_charges/upload")
    }
  }

  it should "GBU planner should skip metadata in sem hash" in {
    val firstGb = buildGroupBy()

    // Create a second GroupBy with different metadata
    val secondGb = buildGroupBy()
    secondGb.metaData = firstGb.metaData.deepCopy()
    secondGb.metaData.name = "secondGroupBy"
    secondGb.metaData.samplePercent = 0.5 // Different sample percent

    val firstPlanner = GroupByUploadPlanner(firstGb)
    val secondPlanner = GroupByUploadPlanner(secondGb)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "GB planner should produce same semantic hash with different executionInfo" in {
    val firstGB = buildGroupBy()

    val secondGB = buildGroupBy()
    secondGB.metaData = firstGB.metaData.deepCopy()
    secondGB.metaData.executionInfo = firstGB.metaData.executionInfo.deepCopy()
    secondGB.metaData.executionInfo.env =
      new EnvironmentVariables().setCommon(Map("foo" -> "bar", "baz" -> "qux").asJava)

    val firstPlanner = GroupByUploadPlanner(firstGB)
    val secondPlanner = GroupByUploadPlanner(secondGB)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata (including executionInfo) is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }
}

object GroupByUploadPlannerTest {
  def buildGroupBy(): GroupBy = {
    val eventsTable = "my_user_events"
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.COUNT, "charges", Seq(WindowUtils.Unbounded))
    )
    Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
      keyColumns = Seq("user"),
      aggregations = aggregations,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "user_charges"),
      accuracy = Accuracy.TEMPORAL
    )
  }
}
