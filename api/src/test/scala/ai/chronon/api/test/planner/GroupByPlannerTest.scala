package ai.chronon.api.test.planner

import ai.chronon.api.{Accuracy, Aggregation, Builders, EnvironmentVariables, GroupBy, Operation, PartitionSpec}
import ai.chronon.api.Extensions.{MetadataOps, WindowUtils}
import ai.chronon.api.planner.{GroupByPlanner, LocalRunner}
import ai.chronon.api.test.planner.GroupByPlannerTest.buildGroupBy
import ai.chronon.planner.{ConfPlan, Mode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

class GroupByPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def validateGBPlan(plan: ConfPlan): Unit = {
    // Should create plan successfully with backfill, upload, and uploadToKV nodes
    plan.nodes.asScala should have size 3

    // Find the upload node, backfill node, and uploadToKV node
    val uploadNode = plan.nodes.asScala.find(_.content.isSetGroupByUpload)
    val backfillNode = plan.nodes.asScala.find(_.content.isSetGroupByBackfill)
    val uploadToKVNode = plan.nodes.asScala.find(_.content.isSetGroupByUploadToKV)

    uploadNode should be(defined)
    backfillNode should be(defined)
    uploadToKVNode should be(defined)

    // Upload node should have content
    uploadNode.get.content should not be null
    uploadNode.get.content.getGroupByUpload should not be null
    uploadNode.get.content.getGroupByUpload.groupBy should not be null

    // Backfill node should have content
    backfillNode.get.content should not be null
    backfillNode.get.content.getGroupByBackfill should not be null
    backfillNode.get.content.getGroupByBackfill.groupBy should not be null

    // UploadToKV node should have content
    uploadToKVNode.get.content should not be null
    uploadToKVNode.get.content.getGroupByUploadToKV should not be null
    uploadToKVNode.get.content.getGroupByUploadToKV.groupBy should not be null

    plan.terminalNodeNames.asScala.size shouldBe 2
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) shouldBe true
    plan.terminalNodeNames.containsKey(Mode.BACKFILL) shouldBe true
  }

  it should "always plan nonzero step days" in {
    val groupBy = buildGroupBy()
    val plannerWithNonZeroStepDays = GroupByPlanner(groupBy)
    val plan = plannerWithNonZeroStepDays.buildPlan
    plan.nodes.asScala.foreach((node) => node.metaData.executionInfo.stepDays shouldNot be(0))
  }

  it should "GB planner handles valid confs" in {

    val runfilesDir = System.getenv("RUNFILES_DIR")
    val gbRootDir = Paths.get(runfilesDir, "chronon/spark/src/test/resources/canary/compiled/group_bys")

    val gbConfs = LocalRunner.parseConfs[ai.chronon.api.GroupBy](gbRootDir.toString)

    val planners = gbConfs.map(new GroupByPlanner(_))

    planners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          validateGBPlan(plan)
        }
      }
  }

  it should "GB planner should create valid plans" in {
    val gb = buildGroupBy()

    val planner = new GroupByPlanner(gb)

    noException should be thrownBy {
      val plan = planner.buildPlan
      validateGBPlan(plan)
      plan.terminalNodeNames.asScala(Mode.DEPLOY) should equal("user_charges/uploadToKV")
      plan.terminalNodeNames.asScala(Mode.BACKFILL) should equal("user_charges/backfill")
    }
  }

  it should "GB planner should skip metadata in sem hash" in {
    val firstGb = buildGroupBy()

    // Create a second GroupBy with different metadata
    val secondGb = buildGroupBy()
    secondGb.metaData = firstGb.metaData.deepCopy()
    secondGb.metaData.name = "secondGroupBy"
    secondGb.metaData.samplePercent = 0.5 // Different sample percent

    val firstPlanner = new GroupByPlanner(firstGb)
    val secondPlanner = new GroupByPlanner(secondGb)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata is excluded (for upload nodes)
    val firstUploadHash = firstPlan.nodes.asScala.find(_.content.isSetGroupByUpload).get.semanticHash
    val secondUploadHash = secondPlan.nodes.asScala.find(_.content.isSetGroupByUpload).get.semanticHash
    firstUploadHash should equal(secondUploadHash)
  }

  it should "GB planner should produce same semantic hash with different executionInfo" in {
    val firstGB = buildGroupBy()

    val secondGB = buildGroupBy()
    secondGB.metaData = firstGB.metaData.deepCopy()
    secondGB.metaData.executionInfo = firstGB.metaData.executionInfo.deepCopy()
    secondGB.metaData.executionInfo.env =
      new EnvironmentVariables().setCommon(Map("foo" -> "bar", "baz" -> "qux").asJava)

    val firstPlanner = new GroupByPlanner(firstGB)
    val secondPlanner = new GroupByPlanner(secondGB)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata (including executionInfo) is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "GB planner uploadToKV node should have correct table dependencies" in {
    val gb = buildGroupBy()
    val planner = new GroupByPlanner(gb)
    val plan = planner.buildPlan
    validateGBPlan(plan)

    val uploadToKVNode = plan.nodes.asScala.find(_.content.isSetGroupByUploadToKV).get
    val executionInfo = uploadToKVNode.metaData.executionInfo
    val tableDeps = executionInfo.tableDependencies.asScala

    // Should have exactly one table dependency
    tableDeps should have size 1

    val tableDep = tableDeps.head
    // Validate table dependency references the upload table
    tableDep.tableInfo.table should equal(gb.metaData.uploadTable)

    // Validate partition specifications match the test partition spec
    tableDep.tableInfo.partitionColumn should equal(testPartitionSpec.column)
    tableDep.tableInfo.partitionFormat should equal(testPartitionSpec.format)

    // Validate offsets are set to zero for upload scenarios
    tableDep.startOffset should not be null
    tableDep.endOffset should not be null
  }
}

object GroupByPlannerTest {
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
