package ai.chronon.spark.test.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions._

import ai.chronon.spark.batch._
import ai.chronon.api.{planner, _}
import ai.chronon.orchestration.JoinBootstrapNode
import ai.chronon.orchestration.JoinDerivationNode
import ai.chronon.orchestration.JoinPartNode
import ai.chronon.orchestration.JoinMergeNode
import ai.chronon.orchestration.SourceWithFilterNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import ai.chronon.spark.test.{DataFrameGen, TableTestUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import ai.chronon.spark.catalog.TableUtils

class ModularJoinTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  val spark: SparkSession = submission.SparkSessionBuilder.build("ModularJoinTest", local = true)
  private implicit val tableUtils: TableTestUtils = TableTestUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = tableUtils.partitionSpec.before(monthAgo)

  private val namespace = "test_namespace_jointest_modular"
  tableUtils.createDatabase(namespace)

  it should "test a join with bootstrap/derivation/external part/events/entity" in {
    val dollarTransactions = List(
      Column("user", StringType, 10),
      Column("user_name", api.StringType, 10),
      Column("ts", LongType, 200),
      Column("amount_dollars", LongType, 1000)
    )

    val rupeeTransactions = List(
      Column("user", StringType, 10),
      Column("user_name", api.StringType, 10),
      Column("ts", LongType, 200),
      Column("amount_rupees", LongType, 70000)
    )

    val dollarTable = s"$namespace.dollar_transactions"
    val rupeeTable = s"$namespace.rupee_transactions"
    spark.sql(s"DROP TABLE IF EXISTS $dollarTable")
    spark.sql(s"DROP TABLE IF EXISTS $rupeeTable")
    DataFrameGen.entities(spark, dollarTransactions, 600, partitions = 200).save(dollarTable, Map("tblProp1" -> "1"))
    DataFrameGen.entities(spark, rupeeTransactions, 500, partitions = 80).save(rupeeTable)

    val dollarSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars", "user_name", "user"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      snapshotTable = dollarTable
    )

    val dollarEventSource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars", "user_name", "user"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      table = dollarTable
    )

    //println("Rupee Source start partition $month")
    val rupeeSource =
      Builders.Source.entities(
        query = Builders.Query(
          selects = Map("ts" -> "ts",
                        "amount_dollars" -> "CAST(amount_rupees/70 as long)",
                        "user_name" -> "user_name",
                        "user" -> "user"),
          startPartition = monthAgo,
          setups = Seq(
            "create temporary function temp_replace_right_b as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          )
        ),
        snapshotTable = rupeeTable
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(dollarSource, rupeeSource),
      keyColumns = Seq("user", "user_name"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )

    val groupBy2 = Builders.GroupBy(
      sources = Seq(dollarEventSource),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount_dollars")),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )

    val queriesSchema = List(
      Column("user_name", api.StringType, 10),
      Column("user", api.StringType, 10)
    )

    val queryTable = s"$namespace.queries"
    DataFrameGen
      .events(spark, queriesSchema, 3000, partitions = 180, partitionColumn = Some("date"))
      .save(queryTable, partitionColumns = Seq("date"))

    // Make bootstrap part and table
    val bootstrapSourceTable = s"$namespace.bootstrap"
    val bootstrapCol = "unit_test_user_transactions_amount_dollars_sum_10d"
    tableUtils
      .loadTable(queryTable)
      .select(
        col("user"),
        col("ts"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as(bootstrapCol),
        col("date").as("ds")
      )
      .save(bootstrapSourceTable)

    val bootstrapGroupBy = Builders.GroupBy(
      sources = Seq(dollarSource, rupeeSource),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(10, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )

    val bootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("user", "ts", "unit_test_user_transactions_amount_dollars_sum_10d"),
        startPartition = start,
        endPartition = today
      ),
      table = s"$namespace.bootstrap",
      keyColumns = Seq("user", "ts")
    )

    val jp1 = Builders.JoinPart(groupBy = groupBy, keyMapping = Map("user_name" -> "user", "user" -> "user_name"))

    val jp2 = Builders.JoinPart(groupBy = groupBy2)

    val returnOneSource = Builders.ExternalSource(
      metadata = Builders.MetaData(
        name = "return_one"
      ),
      keySchema = StructType("key_one", Array(StructField("key_number", IntType))),
      valueSchema = StructType("value_one", Array(StructField("value_number", IntType)))
    )

    val joinConf: ai.chronon.api.Join = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          startPartition = start,
          setups = Seq(
            "create temporary function temp_replace_left as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          ),
          partitionColumn = "date"
        ),
        table = queryTable
      ),
      joinParts = Seq(jp1, jp2, Builders.JoinPart(groupBy = bootstrapGroupBy)),
      bootstrapParts = Seq(bootstrapPart), // ext_return_one_number
      derivations = Seq(
        Builders.Derivation(
          "ratio_derivation",
          "unit_test_user_transactions_amount_dollars_sum / (COALESCE(unit_test_user_transactions_amount_dollars_sum_30d, 0) + 1)"),
        Builders.Derivation("external_coalesce", "COALESCE(ext_return_one_value_number, 1)")
      ),
      externalParts = Seq(Builders.ExternalPart(returnOneSource)),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    val leftSourceWithFilter = new SourceWithFilterNode().setSource(joinConf.left)

    // First run the SourceJob associated with the left
    // Compute source table name using utility function
    val sourceOutputTable = JoinUtils.computeFullLeftSourceTableName(joinConf)

    println(s"Source output table: $sourceOutputTable")

    // Split the output table to get namespace and name
    val sourceParts = sourceOutputTable.split("\\.", 2)
    val sourceNamespace = sourceParts(0)
    val sourceName = sourceParts(1)

    // Create metadata for source job
    val sourceMetaData = new api.MetaData()
      .setName(sourceName)
      .setOutputNamespace(sourceNamespace)

    // Set metadata on source node
    leftSourceWithFilter.setMetaData(sourceMetaData)

    val sourceJobRange = new DateRange()
      .setStartDate(start)
      .setEndDate(today)

    val sourceRunner = new SourceJob(leftSourceWithFilter, sourceJobRange)
    sourceRunner.run()
    tableUtils.sql(s"SELECT * FROM $sourceOutputTable").show()
    val sourceExpected = spark.sql(s"SELECT *, date as ds FROM $queryTable WHERE date >= '$start' AND date <= '$today'")
    val sourceComputed = tableUtils.sql(s"SELECT * FROM $sourceOutputTable").drop("ts_ds")
    val diff = Comparison.sideBySide(sourceComputed, sourceExpected, List("user_name", "user", "ts"))
    if (diff.count() > 0) {
      println(s"Actual count: ${sourceComputed.count()}")
      println(s"Expected count: ${sourceExpected.count()}")
      println(s"Diff count: ${diff.count()}")
      println("diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())

    // Now run the bootstrap part to get the bootstrap table (one of the joinParts)
    val bootstrapOutputTable = joinConf.metaData.bootstrapTable
    val bootstrapJobRange = new DateRange()
      .setStartDate(start)
      .setEndDate(today)

    // Split bootstrap output table
    val bootstrapParts = bootstrapOutputTable.split("\\.", 2)
    val bootstrapNamespace = bootstrapParts(0)
    val bootstrapName = bootstrapParts(1)

    // Create metadata for bootstrap job
    val bootstrapMetaData = new api.MetaData()
      .setName(bootstrapName)
      .setOutputNamespace(bootstrapNamespace)

    val bootstrapNode = new JoinBootstrapNode()
      .setJoin(joinConf)
      .setMetaData(bootstrapMetaData)

    val bsj = new JoinBootstrapJob(bootstrapNode, bootstrapJobRange)
    bsj.run()
    val sourceCount = tableUtils.sql(s"SELECT * FROM $sourceOutputTable").count()
    val bootstrapCount = tableUtils.sql(s"SELECT * FROM $bootstrapOutputTable").count()
    assertEquals(sourceCount, bootstrapCount)
    val boostrapSchema = tableUtils.sql(s"SELECT * FROM $bootstrapOutputTable").schema.map(_.name)
    val expectedSchema =
      Seq(
        "user",
        "ts",
        "user_name",
        "ts_ds",
        "matched_hashes",
        "unit_test_user_transactions_amount_dollars_sum_10d",
        "key_number",
        "ext_return_one_value_number",
        "ds"
      )
    assertEquals(expectedSchema, boostrapSchema)
    tableUtils.sql(s"SELECT * FROM $bootstrapOutputTable").show()

    // Now run the join part job that *does not* have a bootstrap
    // Use RelevantLeftForJoinPart to get the full table name (including namespace)
    val joinPart1TableName = planner.RelevantLeftForJoinPart.partTableName(joinConf, jp1)
    val outputNamespace = joinConf.metaData.outputNamespace
    val joinPart1FullTableName = planner.RelevantLeftForJoinPart.fullPartTableName(joinConf, jp1)

    val joinPartJobRange = new DateRange()
      .setStartDate(start)
      .setEndDate(today)

    // Create metadata with name and namespace directly
    val metaData = new api.MetaData()
      .setName(joinPart1TableName)
      .setOutputNamespace(outputNamespace)

    val joinPartNode = new JoinPartNode()
      .setLeftSourceTable(sourceOutputTable)
      .setLeftDataModel(joinConf.getLeft.dataModel)
      .setJoinPart(jp1)
      .setMetaData(metaData)

    val joinPartJob = new JoinPartJob(joinPartNode, joinPartJobRange)
    joinPartJob.run()
    tableUtils.sql(s"SELECT * FROM $joinPart1FullTableName").show()

    // Now run the join part job that *does not* have a bootstrap
    // Use RelevantLeftForJoinPart to get the appropriate output table name
    val joinPart2TableName = planner.RelevantLeftForJoinPart.partTableName(joinConf, jp2)
    val joinPart2FullTableName = planner.RelevantLeftForJoinPart.fullPartTableName(joinConf, jp2)

    val metaData2 = new api.MetaData()
      .setName(joinPart2TableName)
      .setOutputNamespace(outputNamespace)

    val joinPartNode2 = new JoinPartNode()
      .setLeftSourceTable(sourceOutputTable)
      .setLeftDataModel(joinConf.getLeft.dataModel)
      .setJoinPart(jp2)
      .setMetaData(metaData2)

    val joinPart2Job = new JoinPartJob(joinPartNode2, joinPartJobRange)
    joinPart2Job.run()
    tableUtils.sql(s"SELECT * FROM $joinPart2FullTableName").show()

    // Skip the joinPart that does have a bootstrap, and go straight to merge job
    val mergeJobOutputTable = joinConf.metaData.outputTable

    val mergeJobRange = new DateRange()
      .setStartDate(start)
      .setEndDate(today)

    // Create metadata for merge job
    val mergeMetaData = new api.MetaData()
      .setName(joinConf.metaData.name)
      .setOutputNamespace(namespace)

    val mergeNode = new JoinMergeNode()
      .setJoin(joinConf)
      .setMetaData(mergeMetaData)

    val finalJoinJob = new MergeJob(mergeNode, mergeJobRange, Seq(jp1, jp2))
    finalJoinJob.run()
    tableUtils.sql(s"SELECT * FROM $mergeJobOutputTable").show()

    // Now run the derivations job
    val derivationOutputTable = s"$namespace.test_user_transaction_features_v1_derived"

    val derivationRange = new DateRange()
      .setStartDate(start)
      .setEndDate(today)

    // Split derivation output table
    val derivationParts = derivationOutputTable.split("\\.", 2)
    val derivationNamespace = derivationParts(0)
    val derivationName = derivationParts(1)

    // Create metadata for derivation job
    val derivationMetaData = new api.MetaData()
      .setName(derivationName)
      .setOutputNamespace(derivationNamespace)

    val derivationNode = new JoinDerivationNode()
      .setJoin(joinConf)
      .setMetaData(derivationMetaData)

    val joinDerivationJob = new JoinDerivationJob(derivationNode, derivationRange)
    joinDerivationJob.run()
    tableUtils.sql(s"SELECT * FROM $derivationOutputTable").show()

    val expectedQuery = s"""
                |WITH
                |   queries AS (
                |     SELECT user_name,
                |         user,
                |         ts,
                |         date as ds
                |     from $queryTable
                |     where user_name IS NOT null
                |         AND user IS NOT NULL
                |         AND ts IS NOT NULL
                |         AND date IS NOT NULL
                |         AND date >= '$start'
                |         AND date <= '$today')
                |  SELECT
                |    queries.user,
                |    queries.ts,
                |    queries.ds,
                |    SUM(IF(dollar.ts < queries.ts, dollar.amount_dollars, null)) / 1 as ratio_derivation,
                |    1 as external_coalesce
                |  FROM queries
                |  LEFT OUTER JOIN $dollarTable as dollar
                |  on queries.user == dollar.user
                |  GROUP BY queries.user, queries.ts, queries.ds
                |""".stripMargin
    spark.sql(expectedQuery).show()
    val expected = spark.sql(expectedQuery)
    val computed = spark.sql(s"SELECT user, ts, ds, ratio_derivation, external_coalesce FROM $derivationOutputTable")

    val finalDiff = Comparison.sideBySide(computed, expected, List("user", "ts", "ds"))

    if (finalDiff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println("diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }
}
