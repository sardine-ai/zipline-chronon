package ai.chronon.flink.test

import ai.chronon.flink.AsyncKVStoreWriter
import ai.chronon.flink.types.AvroCodecOutput
import ai.chronon.online.Api
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.test.TaggedFilterSuite
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.DataStreamUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

class AsyncKVStoreWriterTest extends AnyFlatSpec with TaggedFilterSuite  {

  val eventTs = 1519862400075L

  def createKVRequest(key: String, value: String, dataset: String, ts: Long): PutRequest =
    PutRequest(key.getBytes, value.getBytes, dataset, Some(ts))

  it should "write successfully" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[AvroCodecOutput] = env
      .fromCollection(
        Range(0, 5).map(i => new AvroCodecOutput(i.toString.getBytes, "test".getBytes, "my_dataset", eventTs))
      )

    val mockApi = mock[Api]
    val withRetries =
      AsyncKVStoreWriter
        .withUnorderedWaits(
          source,
          new MockAsyncKVStoreWriter(Seq(true), mockApi, "testFG"),
          "testFG"
        )
    val result = new DataStreamUtils(withRetries).collect.toSeq
    assert(result.nonEmpty, "Expect result set to be non-empty")
    assert(result.map(_.tsMillis).forall(_ == eventTs))
  }

  // ensure that if we get an event that would cause the operator to throw an exception,
  // we don't crash the app
  it should "handle poison pill writes" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[AvroCodecOutput] = env
      .fromCollection(
        Range(0, 5).map(i => new AvroCodecOutput(i.toString.getBytes, "test".getBytes, "my_dataset", eventTs))
      )

    val mockApi = mock[Api]
    val withRetries =
      AsyncKVStoreWriter
        .withUnorderedWaits(
          source,
          new MockAsyncKVStoreWriter(Seq(false), mockApi, "testFG"),
          "testFG"
        )
    val result = new DataStreamUtils(withRetries).collect.toSeq
    assert(result.nonEmpty, "Expect result set to be non-empty")
    assert(result.map(_.tsMillis).forall(_ == eventTs))
  }

  override def tagName: String = "asyncKVStoreWriterTest"
}
