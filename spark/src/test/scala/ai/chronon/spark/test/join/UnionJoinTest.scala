package ai.chronon.spark.test.join

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.join.UnionJoin
import ai.chronon.spark.test.DataFrameGen
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class UnionJoinTest extends BaseJoinTest {

  it should "test events events temporal case for UnionJoin.computeJoinAndSave" in {

    val viewsSchema = List(
      Column("user", api.StringType, 1),
      Column("item", api.StringType, 1),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_union_temporal"
    DataFrameGen
      .events(spark, viewsSchema, count = 10000, partitions = 20)
      .save(viewsTable, Map("tblProp1" -> "1"))

    val viewsDf = tableUtils.loadTable(viewsTable)

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

    // left side
    val itemQueries = List(Column("item", api.StringType, 1))
    val itemQueriesTable = s"$namespace.item_queries_union_temporal"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 10)

    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable)

    val queriesDf = tableUtils.loadTable(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(20, TimeUnit.DAYS))
    val dateRange = PartitionRange(start, today)(tableUtils.partitionSpec)

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData =
        Builders.MetaData(name = s"test.item_temporal_features.union_join", namespace = namespace, team = "item_team")
    )

    // Test UnionJoin.computeJoinAndSave method

    UnionJoin.computeJoinAndSave(joinConf, dateRange)

    val outputDf = tableUtils.loadTable(joinConf.metaData.outputTable)

    val outputData = outputDf.where("item IS NOT NULL and ts IS NOT NULL").collect()
    val queriesData = queriesDf.where("item IS NOT NULL and ts IS NOT NULL").collect()
    outputData.length shouldBe queriesData.length
  }
}
