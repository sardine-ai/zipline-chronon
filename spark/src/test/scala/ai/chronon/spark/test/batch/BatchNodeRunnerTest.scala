package ai.chronon.spark.batch

import ai.chronon.api.{Accuracy, Builders, Operation, PartitionRange, TimeUnit, Window}
import ai.chronon.planner.{MonolithJoinNode, NodeContent}
import ai.chronon.spark.test.join.BaseJoinTest
import ai.chronon.spark.Extensions._
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success, Try}

class BatchNodeRunnerTest extends BaseJoinTest with Matchers {

  private val runfilesDir = System.getenv("RUNFILES_DIR")
  private val canaryResourcePath = s"$runfilesDir/chronon/spark/src/test/resources/canary/compiled"
  private val joinConfigPath = s"$canaryResourcePath/joins/gcp/training_set.v1_test"
  private val groupByConfigPath = s"$canaryResourcePath/group_bys/gcp/purchases.v1_test"

  "BatchNodeRunner" should "validate monolith join node configuration without execution" in {
    val viewsTable = s"$namespace.view_union_temporal"
    val viewsSource = Builders.Source.events(
      table = viewsTable,
      topic = "",
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"),
                             startPartition = tableUtils.partitionSpec.minus(today, new Window(20, TimeUnit.DAYS)))
    )

    val viewsGroupBy = Builders
      .GroupBy(
        sources = Seq(viewsSource),
        keyColumns = Seq("item"),
        aggregations = Seq(
          Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
          Builders.Aggregation(
            operation = Operation.LAST_K,
            argMap = Map("k" -> "50"),
            inputColumn = "time_spent_ms",
            windows = Seq(new Window(2, TimeUnit.DAYS))
          )
        ),
        metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace)
      )
      .setAccuracy(Accuracy.TEMPORAL)

    val itemQueriesTable = s"$namespace.item_queries_union_temporal"
    val start = tableUtils.partitionSpec.minus(today, new Window(20, TimeUnit.DAYS))
    val dateRange = PartitionRange(start, today)(tableUtils.partitionSpec)

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData =
        Builders.MetaData(name = s"item_temporal_features_union_join", namespace = namespace, team = "item_team")
    )

    val joinNodeContent = new NodeContent()
    joinNodeContent.setMonolithJoin(new MonolithJoinNode().setJoin(joinConf))

    // Test configuration validation without execution
    joinNodeContent.isSetMonolithJoin shouldBe true
    joinNodeContent.getMonolithJoin.getJoin.metaData.name shouldBe "item_temporal_features_union_join"
    joinNodeContent.getMonolithJoin.getJoin.joinParts should have size 1
  }

  it should "properly set up monolith join configuration for execution" in {
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(), table = "test_table"),
      joinParts = Seq(),
      metaData = Builders.MetaData(name = "test_join", namespace = namespace)
    )

    val monolithJoin = new MonolithJoinNode().setJoin(joinConf)
    val nodeContent = new NodeContent()
    nodeContent.setMonolithJoin(monolithJoin)

    val start = tableUtils.partitionSpec.minus(today, new Window(1, TimeUnit.DAYS))
    val range = PartitionRange(start, today)(tableUtils.partitionSpec)

    // Verify configuration is set up correctly without executing
    nodeContent.isSetMonolithJoin shouldBe true
    nodeContent.getMonolithJoin.getJoin.metaData.name shouldBe "test_join"
    range.start shouldBe start
    range.end shouldBe today
  }

  it should "throw exception for unsupported NodeContent type" in {
    val nodeContent = new NodeContent()
    // Not setting any specific type, should cause unsupported operation

    val metadata = Builders.MetaData(name = "test", namespace = namespace)
    val start = tableUtils.partitionSpec.minus(today, new Window(1, TimeUnit.DAYS))
    val range = PartitionRange(start, today)(tableUtils.partitionSpec)

    an[UnsupportedOperationException] should be thrownBy {
      BatchNodeRunner.run(metadata, nodeContent, range, tableUtils)
    }
  }

  it should "require partition range to be defined" in {
    val metadata = Builders.MetaData(name = "test", namespace = namespace)
    val nodeContent = new NodeContent()

    an[IllegalArgumentException] should be thrownBy {
      BatchNodeRunner.run(metadata, nodeContent, None)
    }
  }

  it should "load join configuration correctly" in {
    val (metadata, nodeContent) = BatchNodeRunner.loadNodeContent(joinConfigPath, "join")

    metadata.name shouldBe "gcp.training_set.v1_test"
    nodeContent.isSetMonolithJoin shouldBe true
    nodeContent.getMonolithJoin.getJoin.metaData.name shouldBe "gcp.training_set.v1_test"
  }

  it should "handle staging query config type validation" in {
    // Test that staging query configurations can be loaded successfully
    // The groupByConfigPath actually contains a staging query configuration
    val (metadata, nodeContent) = BatchNodeRunner.loadNodeContent(groupByConfigPath, "staging_query")

    metadata.name shouldBe "gcp.purchases.v1_test"
    nodeContent.isSetStagingQuery shouldBe true
    nodeContent.getStagingQuery.getStagingQuery.metaData.name shouldBe "gcp.purchases.v1_test"
  }

  it should "throw exception for unsupported config type" in {
    an[IllegalArgumentException] should be thrownBy {
      BatchNodeRunner.loadNodeContent(joinConfigPath, "unsupported_type")
    }
  }

  it should "successfully parse and validate configuration from args without execution" in {
    val startStr = "2023-11-01"
    val endStr = "2023-11-02"

    // Test configuration loading and argument interpretation without actual execution
    val (metadata, nodeContent) = BatchNodeRunner.loadNodeContent(joinConfigPath, "join")
    val range = PartitionRange(startStr, endStr)(tableUtils.partitionSpec)

    // Verify configuration was loaded correctly
    metadata.name shouldBe "gcp.training_set.v1_test"
    nodeContent.isSetMonolithJoin shouldBe true
    nodeContent.getMonolithJoin.getJoin.metaData.name shouldBe "gcp.training_set.v1_test"

    // Verify partition range was created correctly
    range.start shouldBe startStr
    range.end shouldBe endStr
  }

  it should "handle configuration loading failure gracefully" in {
    // Test that loadNodeContent fails with nonexistent file
    an[Exception] should be thrownBy {
      BatchNodeRunner.loadNodeContent("nonexistent_file.v1", "join")
    }
  }

  it should "interpret CLI arguments and set up batch execution correctly" in {
    val args = Array(
      "--conf-path",
      joinConfigPath,
      "--start-ds",
      "2023-01-01",
      "--end-ds",
      "2023-01-02",
      "--conf-type",
      "join",
      "--conf-mode",
      "batch"
    )

    // Parse command line arguments using throwError to catch validation issues
    val result = Try {
      org.rogach.scallop.throwError.withValue(true) {
        new BatchNodeRunnerArgs(args)
      }
    }
    result shouldBe a[Success[_]]
    val batchArgs = result.get

    // Verify all arguments are parsed correctly
    batchArgs.confPath() shouldBe joinConfigPath
    batchArgs.startDs() shouldBe "2023-01-01"
    batchArgs.endDs() shouldBe "2023-01-02"
    batchArgs.confType() shouldBe "join"
    batchArgs.confMode() shouldBe "batch"

    // Test that configuration can be loaded with these arguments
    val (metadata, nodeContent) = BatchNodeRunner.loadNodeContent(batchArgs.confPath(), batchArgs.confType())
    val range = PartitionRange(batchArgs.startDs(), batchArgs.endDs())(tableUtils.partitionSpec)

    // Verify the setup is complete and ready for execution
    metadata.name shouldBe "gcp.training_set.v1_test"
    nodeContent.isSetMonolithJoin shouldBe true
    range.start shouldBe "2023-01-01"
    range.end shouldBe "2023-01-02"
  }

  it should "parse command line arguments correctly" in {
    val args = Array(
      "--conf-path",
      joinConfigPath,
      "--start-ds",
      "2023-01-01",
      "--end-ds",
      "2023-01-02",
      "--conf-type",
      "join",
      "--conf-mode",
      "batch"
    )

    val result = Try {
      org.rogach.scallop.throwError.withValue(true) {
        new BatchNodeRunnerArgs(args)
      }
    }
    result shouldBe a[Success[_]]
    val batchArgs = result.get

    batchArgs.confPath() shouldBe joinConfigPath
    batchArgs.startDs() shouldBe "2023-01-01"
    batchArgs.endDs() shouldBe "2023-01-02"
    batchArgs.confType() shouldBe "join"
    batchArgs.confMode() shouldBe "batch"
  }

  it should "validate confType choices" in {
    val validArgs = Array(
      "--conf-path",
      joinConfigPath,
      "--start-ds",
      "2023-01-01",
      "--end-ds",
      "2023-01-02",
      "--conf-type",
      "staging_query",
      "--conf-mode",
      "batch"
    )

    val validResult = Try {
      org.rogach.scallop.throwError.withValue(true) {
        new BatchNodeRunnerArgs(validArgs)
      }
    }
    validResult shouldBe a[Success[_]]

    val invalidArgs = Array(
      "--conf-path",
      joinConfigPath,
      "--start-ds",
      "2023-01-01",
      "--end-ds",
      "2023-01-02",
      "--conf-type",
      "invalid_type",
      "--conf-mode",
      "batch"
    )

    val invalidResult = Try {
      org.rogach.scallop.throwError.withValue(true) {
        new BatchNodeRunnerArgs(invalidArgs)
      }
    }
    invalidResult shouldBe a[Failure[_]]
  }

  it should "validate confMode choices" in {
    val validArgs = Array(
      "--conf-path",
      joinConfigPath,
      "--start-ds",
      "2023-01-01",
      "--end-ds",
      "2023-01-02",
      "--conf-type",
      "join",
      "--conf-mode",
      "streaming"
    )

    val validResult = Try {
      org.rogach.scallop.throwError.withValue(true) {
        new BatchNodeRunnerArgs(validArgs)
      }
    }
    validResult shouldBe a[Success[_]]

    val invalidArgs = Array(
      "--conf-path",
      joinConfigPath,
      "--start-ds",
      "2023-01-01",
      "--end-ds",
      "2023-01-02",
      "--conf-type",
      "join",
      "--conf-mode",
      "invalid_mode"
    )

    val invalidResult = Try {
      org.rogach.scallop.throwError.withValue(true) {
        new BatchNodeRunnerArgs(invalidArgs)
      }
    }
    invalidResult shouldBe a[Failure[_]]
  }
}
