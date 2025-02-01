package ai.chronon.flink.test

import ai.chronon.flink.FlinkJob
import ai.chronon.flink.SparkExpressionEvalFn
import ai.chronon.flink.types.TimestampedIR
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.flink.types.WriteResponse
import ai.chronon.online.Api
import ai.chronon.online.GroupByServingInfoParsed
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Encoders
import org.mockito.Mockito.withSettings
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.jdk.CollectionConverters.asScalaBufferConverter

class FlinkJobIntegrationTest extends AnyFlatSpec with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(8)
      .setNumberTaskManagers(1)
      .build)

  // Decode a PutRequest into a TimestampedTile
  def avroConvertPutRequestToTimestampedTile[T](
      in: WriteResponse,
      groupByServingInfoParsed: GroupByServingInfoParsed
  ): TimestampedTile = {
    // Decode the key bytes into a GenericRecord
    val tileBytes = in.valueBytes
    val record = groupByServingInfoParsed.keyCodec.decode(in.keyBytes)

    // Get all keys we expect to be in the GenericRecord
    val decodedKeys: List[String] =
      groupByServingInfoParsed.groupBy.keyColumns.asScala.map(record.get(_).toString).toList

    val tsMills = in.tsMillis
    new TimestampedTile(decodedKeys, tileBytes, tsMills)
  }

  // Decode a TimestampedTile into a TimestampedIR
  def avroConvertTimestampedTileToTimestampedIR(timestampedTile: TimestampedTile,
                                                groupByServingInfoParsed: GroupByServingInfoParsed): TimestampedIR = {
    val tileIR = groupByServingInfoParsed.tiledCodec.decodeTileIr(timestampedTile.tileBytes)
    new TimestampedIR(tileIR._1, Some(timestampedTile.latestTsMillis))
  }

  before {
    flinkCluster.before()
    CollectSink.values.clear()
  }

  after {
    flinkCluster.after()
    CollectSink.values.clear()
  }

  it should "flink job end to end" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, 1699366993123L),
      E2ETestEvent("test2", 13, 1.6, 1699366993124L),
      E2ETestEvent("test3", 14, 1.7, 1699366993125L)
    )

    val source = new E2EEventSource(elements)
    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val encoder = Encoders.product[E2ETestEvent]

    val outputSchema = new SparkExpressionEvalFn(encoder, groupBy).getOutputSchema

    val groupByServingInfoParsed =
      FlinkTestUtils.makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)
    val mockApi = mock[Api](withSettings().serializable())
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), mockApi, "testFlinkJobEndToEndFG")
    val job = new FlinkJob[E2ETestEvent](source, writerFn, groupByServingInfoParsed, encoder, 2)

    job.runGroupByJob(env).addSink(new CollectSink)

    env.execute("FlinkJobIntegrationTest")

    // capture the datastream of the 'created' timestamps of all the written out events
    val writeEventCreatedDS = CollectSink.values.asScala

    writeEventCreatedDS.size shouldBe elements.size
    // check that the timestamps of the written out events match the input events
    // we use a Set as we can have elements out of order given we have multiple tasks

    writeEventCreatedDS.map(_.tsMillis).toSet shouldBe elements.map(_.created).toSet
    // check that all the writes were successful
    writeEventCreatedDS.map(_.status) shouldBe Seq(true, true, true)
  }

  it should "tiled flink job end to end" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // Create some test events with multiple different ids so we can check if tiling/pre-aggregation works correctly
    // for each of them.
    val id1Elements = Seq(E2ETestEvent(id = "id1", int_val = 1, double_val = 1.5, created = 1L),
                          E2ETestEvent(id = "id1", int_val = 1, double_val = 2.5, created = 2L))
    val id2Elements = Seq(E2ETestEvent(id = "id2", int_val = 1, double_val = 10.0, created = 3L))
    val elements: Seq[E2ETestEvent] = id1Elements ++ id2Elements
    val source = new WatermarkedE2EEventSource(elements)

    // Make a GroupBy that SUMs the double_val of the elements.
    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))

    // Prepare the Flink Job
    val encoder = Encoders.product[E2ETestEvent]
    val outputSchema = new SparkExpressionEvalFn(encoder, groupBy).getOutputSchema
    val groupByServingInfoParsed =
      FlinkTestUtils.makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)
    val mockApi = mock[Api](withSettings().serializable())
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), mockApi, "testTiledFlinkJobEndToEndFG")
    val job = new FlinkJob[E2ETestEvent](source, writerFn, groupByServingInfoParsed, encoder, 2)
    job.runTiledGroupByJob(env).addSink(new CollectSink)

    env.execute("TiledFlinkJobIntegrationTest")

    // capture the datastream of the 'created' timestamps of all the written out events
    val writeEventCreatedDS = CollectSink.values.asScala

    // BASIC ASSERTIONS
    // All elements were processed
    writeEventCreatedDS.size shouldBe elements.size
    // check that the timestamps of the written out events match the input events
    // we use a Set as we can have elements out of order given we have multiple tasks
    writeEventCreatedDS.map(_.tsMillis).toSet shouldBe elements.map(_.created).toSet

    // check that all the writes were successful
    writeEventCreatedDS.map(_.status) shouldBe Seq(true, true, true)

    // Assert that the pre-aggregates/tiles are correct
    // Get a list of the final IRs for each key.
    val finalIRsPerKey: Map[Seq[Any], List[Any]] = writeEventCreatedDS
      .map(writeEvent => {
        // First, we work back from the PutRequest decode it to TimestampedTile and then TimestampedIR
        val timestampedTile =
          avroConvertPutRequestToTimestampedTile(writeEvent, groupByServingInfoParsed)
        val timestampedIR = avroConvertTimestampedTileToTimestampedIR(timestampedTile, groupByServingInfoParsed)

        // We're interested in the the keys, Intermediate Result, and the timestamp for each processed event
        (timestampedTile.keys, timestampedIR.ir.toList, writeEvent.tsMillis)
      })
      .groupBy(_._1) // Group by the keys
      .map((keys) => (keys._1, keys._2.maxBy(_._3)._2)) // pick just the events with largest timestamp

    // Looking back at our test events, we expect the following Intermediate Results to be generated:
    val expectedFinalIRsPerKey = Map(
      List("id1") -> List(4.0), // Add up the double_val of the two 'id1' events
      List("id2") -> List(10.0)
    )

    expectedFinalIRsPerKey shouldBe finalIRsPerKey
  }
}
