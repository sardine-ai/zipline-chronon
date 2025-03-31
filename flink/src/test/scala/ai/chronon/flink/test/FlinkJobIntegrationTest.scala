package ai.chronon.flink.test

import ai.chronon.api.TilingUtils
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.flink.{FlinkJob, SparkExpressionEval, SparkExpressionEvalFn}
import ai.chronon.flink.types.TimestampedIR
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.flink.types.WriteResponse
import ai.chronon.online.{Api, GroupByServingInfoParsed, SparkConversions}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Encoders
import org.mockito.Mockito.withSettings
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.collection.Seq

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
    // Deserialize the TileKey object and pull out the entity key bytes
    val tileKey = TilingUtils.deserializeTileKey(in.keyBytes)
    val keyBytes = tileKey.keyBytes.toScala.toArray.map(_.asInstanceOf[Byte])
    val record = groupByServingInfoParsed.keyCodec.decode(keyBytes)

    // Get all keys we expect to be in the GenericRecord
    val decodedKeys: List[String] =
      groupByServingInfoParsed.groupBy.keyColumns.toScala.map(record.get(_).toString).toList

    val tsMills = in.tsMillis
    new TimestampedTile(decodedKeys.map(_.asInstanceOf[Any]).toJava, tileBytes, tsMills)
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

  it should "tiled flink job end to end" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // Create some test events with multiple different ids so we can check if tiling/pre-aggregation works correctly
    // for each of them.
    // We stick to unique ids (one event per id) as it helps with validation as Flink often sends events out of order.
    val id1Elements = Array(E2ETestEvent(id = "id1", int_val = 1, double_val = 1.5, created = 1L))
    val id2Elements = Array(E2ETestEvent(id = "id2", int_val = 1, double_val = 10.0, created = 2L))
    val id3Elements = Array(E2ETestEvent(id = "id3", int_val = 1, double_val = 2.5, created = 3L))
    val elements: Seq[E2ETestEvent] = id1Elements ++ id2Elements ++ id3Elements

    // Make a GroupBy that SUMs the double_val of the elements.
    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val sparkExpressionEvalFn = new SparkExpressionEvalFn(Encoders.product[E2ETestEvent], groupBy)
    val source = new WatermarkedE2EEventSource(elements, sparkExpressionEvalFn)

    // Prepare the Flink Job
    val encoder = Encoders.product[E2ETestEvent]
    val outputSchema = new SparkExpressionEval(encoder, groupBy).getOutputSchema
    val outputSchemaDataTypes = outputSchema.fields.map { field =>
      (field.name, SparkConversions.toChrononType(field.name, field.dataType))
    }

    val groupByServingInfoParsed =
      FlinkTestUtils.makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)
    val mockApi = mock[Api](withSettings().serializable())
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), mockApi, "testTiledFlinkJobEndToEndFG")
    val job = new FlinkJob(source, outputSchemaDataTypes, writerFn, groupByServingInfoParsed, 2)
    job.runTiledGroupByJob(env).addSink(new CollectSink)

    env.execute("TiledFlinkJobIntegrationTest")

    // capture the datastream of the 'created' timestamps of all the written out events
    val writeEventCreatedDS = CollectSink.values.toScala

    // BASIC ASSERTIONS
    // All elements were processed
    writeEventCreatedDS.size shouldBe elements.size

    // check that the timestamps of the written out events match the input events
    // we use a Set as we can have elements out of order given we have multiple tasks
    writeEventCreatedDS.map(_.tsMillis).toSet shouldBe elements.map(_.created).toSet

    // check that all the writes were successful
    writeEventCreatedDS.map(_.status) shouldBe Seq(true, true, true)

    // Assert that the pre-aggregates/tiles are deserializable
    // Get a list of the final IRs for each key.
    val finalIRsPerKey: Map[Seq[Any], List[Any]] = writeEventCreatedDS
      .map(writeEvent => {
        // First, we work back from the PutRequest decode it to TimestampedTile and then TimestampedIR
        val timestampedTile =
          avroConvertPutRequestToTimestampedTile(writeEvent, groupByServingInfoParsed)
        val timestampedIR = avroConvertTimestampedTileToTimestampedIR(timestampedTile, groupByServingInfoParsed)

        // We're interested in the keys, Intermediate Result, and the timestamp for each processed event
        (timestampedTile.keys, timestampedIR.ir.toList, writeEvent.tsMillis)
      })
      .groupBy(_._1) // Group by the keys
      .map((keys) => (keys._1.toScala, keys._2.maxBy(_._3)._2)) // pick just the events with the largest timestamp

    // As we have unique ids and one event per id, we expect one result per event processed.
    // Looking back at our test events, we expect the following Intermediate Results to be generated:
    val expectedFinalIRsPerKey = Map(
      List("id1") -> List(1.5),
      List("id2") -> List(10.0),
      List("id3") -> List(2.5)
    )

    expectedFinalIRsPerKey shouldBe finalIRsPerKey
  }
}
