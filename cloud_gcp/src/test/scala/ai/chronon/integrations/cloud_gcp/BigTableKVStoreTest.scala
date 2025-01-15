package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.KVStore.GetResponse
import ai.chronon.online.KVStore.ListRequest
import ai.chronon.online.KVStore.PutRequest
import com.google.api.core.ApiFutures
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.rpc.ServerStreamingCallable
import com.google.api.gax.rpc.UnaryCallable
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.bigtable.data.v2.models.Query
import com.google.cloud.bigtable.data.v2.models.Row
import com.google.cloud.bigtable.data.v2.models.RowMutation
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.Mockito.withSettings
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

import java.nio.charset.StandardCharsets
import java.util
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

@RunWith(classOf[JUnit4])
class BigTableKVStoreTest extends AnyFlatSpec with BeforeAndAfter{

  import BigTableKVStore._

  // ugly hacks to wire up the emulator. See: https://stackoverflow.com/questions/32160549/using-junit-rule-with-scalatest-e-g-temporaryfolder
  val _bigtableEmulator: BigtableEmulatorRule = BigtableEmulatorRule.create()
  @Rule
  def bigtableEmulator: BigtableEmulatorRule = _bigtableEmulator

  private var dataClient: BigtableDataClient = _
  private var adminClient: BigtableTableAdminClient = _

  private val projectId = "test-project"
  private val instanceId = "test-instance"

  before {
    // Configure settings to use emulator
    val dataSettings = BigtableDataSettings
      .newBuilderForEmulator(bigtableEmulator.getPort)
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .setCredentialsProvider(NoCredentialsProvider.create())
      .build()

    val adminSettings = BigtableTableAdminSettings
      .newBuilderForEmulator(bigtableEmulator.getPort)
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .setCredentialsProvider(NoCredentialsProvider.create())
      .build()

    // Create clients
    dataClient = BigtableDataClient.create(dataSettings)
    adminClient = BigtableTableAdminClient.create(adminSettings)
  }

  it should "big table creation" in {
    val kvStore = new BigTableKVStoreImpl(dataClient, adminClient)
    val dataset = "test-table"
    kvStore.create(dataset)
    adminClient.listTables().asScala.contains(dataset) shouldBe true
    // another create should not fail
    kvStore.create(dataset)
  }

  // Test write & read of a simple blob dataset
  it should "blob data round trip" in {
    val dataset = "models"
    val kvStore = new BigTableKVStoreImpl(dataClient, adminClient)
    kvStore.create(dataset)

    val key1 = "alice"
    val key2 = "bob"
    // some blob json payloads
    val value1 = """{"name": "alice", "age": 30}"""
    val value2 = """{"name": "bob", "age": 40}"""

    val putReq1 = PutRequest(key1.getBytes, value1.getBytes, dataset, None)
    val putReq2 = PutRequest(key2.getBytes, value2.getBytes, dataset, None)

    val putResults = Await.result(kvStore.multiPut(Seq(putReq1, putReq2)), 1.second)
    putResults shouldBe Seq(true, true)

    // let's try and read these
    val getReq1 = GetRequest(key1.getBytes, dataset, None, None)
    val getReq2 = GetRequest(key2.getBytes, dataset, None, None)

    val getResult1 = Await.result(kvStore.multiGet(Seq(getReq1)), 1.second)
    val getResult2 = Await.result(kvStore.multiGet(Seq(getReq2)), 1.second)

    getResult1.size shouldBe 1
    validateBlobValueExpectedPayload(getResult1.head, value1)
    getResult2.size shouldBe 1
    validateBlobValueExpectedPayload(getResult2.head, value2)
  }

  it should "blob data updates" in {
    val dataset = "models"
    val kvStore = new BigTableKVStoreImpl(dataClient, adminClient)
    kvStore.create(dataset)

    val key1 = "alice"
    // some blob json payloads
    val value = """{"name": "alice", "age": 30}"""

    val putReq = PutRequest(key1.getBytes, value.getBytes, dataset, None)

    val putResults = Await.result(kvStore.multiPut(Seq(putReq)), 1.second)
    putResults shouldBe Seq(true)

    // let's try and read this record
    val getReq = GetRequest(key1.getBytes, dataset, None, None)

    val getResult = Await.result(kvStore.multiGet(Seq(getReq)), 1.second)

    getResult.size shouldBe 1
    validateBlobValueExpectedPayload(getResult.head, value)

    // let's now mutate this record
    val valueUpdated = """{"name": "alice", "age": 35}"""
    val putReqUpdated = PutRequest(key1.getBytes, valueUpdated.getBytes, dataset, None)

    val putResultsUpdated = Await.result(kvStore.multiPut(Seq(putReqUpdated)), 1.second)
    putResultsUpdated shouldBe Seq(true)
    // and read & verify
    val getResultUpdated = Await.result(kvStore.multiGet(Seq(getReq)), 1.second)

    getResultUpdated.size shouldBe 1
    validateBlobValueExpectedPayload(getResultUpdated.head, valueUpdated)
  }

  it should "list with pagination" in {
    val dataset = "models"
    val kvStore = new BigTableKVStoreImpl(dataClient, adminClient)
    kvStore.create(dataset)

    val putReqs = (0 until 100).map { i =>
      val key = s"key-$i"
      val value = s"""{"name": "name-$i", "age": $i}"""
      PutRequest(key.getBytes, value.getBytes, dataset, None)
    }

    val putResults = Await.result(kvStore.multiPut(putReqs), 1.second)
    putResults.foreach(r => r shouldBe true)

    // let's try and read these
    val limit = 10
    val listReq1 = ListRequest(dataset, Map(listLimit -> limit))

    val listResult1 = Await.result(kvStore.list(listReq1), 1.second)
    listResult1.values.isSuccess shouldBe true
    listResult1.resultProps.contains(BigTableKVStore.continuationKey) shouldBe true
    val listValues1 = listResult1.values.get
    listValues1.size shouldBe limit

    // another call, bigger limit
    val limit2 = 1000
    val continuationKey = listResult1.resultProps(BigTableKVStore.continuationKey)
    val listReq2 = ListRequest(dataset, Map(listLimit -> limit2, BigTableKVStore.continuationKey -> continuationKey))
    val listResult2 = Await.result(kvStore.list(listReq2), 1.second)
    listResult2.values.isSuccess shouldBe true
    listResult2.resultProps.contains(BigTableKVStore.continuationKey) shouldBe false
    val listValues2 = listResult2.values.get
    listValues2.size shouldBe (putReqs.size - limit)

    // lets collect all the keys and confirm we got everything
    val allKeys = (listValues1 ++ listValues2).map(v => new String(v.keyBytes, StandardCharsets.UTF_8))
    allKeys.toSet shouldBe putReqs
      .map(r => new String(buildRowKey(r.keyBytes, r.dataset), StandardCharsets.UTF_8))
      .toSet
  }

  it should "multiput failures" in {
    val mockDataClient: BigtableDataClient = mock[BigtableDataClient](withSettings().mockMaker("mock-maker-inline"))
    val mockAdminClient = mock[BigtableTableAdminClient]
    val kvStoreWithMocks = new BigTableKVStoreImpl(mockDataClient, mockAdminClient)

    val failedFuture = ApiFutures.immediateFailedFuture[Void](new RuntimeException("some BT exception on read"))
    when(mockDataClient.mutateRowAsync(any[RowMutation])).thenReturn(failedFuture)

    val dataset = "models"
    val key1 = "alice"
    val key2 = "bob"
    // some blob json payloads
    val value1 = """{"name": "alice", "age": 30}"""
    val value2 = """{"name": "bob", "age": 40}"""

    val putReq1 = PutRequest(key1.getBytes, value1.getBytes, dataset, None)
    val putReq2 = PutRequest(key2.getBytes, value2.getBytes, dataset, None)

    val putResults = Await.result(kvStoreWithMocks.multiPut(Seq(putReq1, putReq2)), 1.second)
    putResults shouldBe Seq(false, false)
  }

  it should "multiget failures" in {
    val mockDataClient: BigtableDataClient = mock[BigtableDataClient](withSettings().mockMaker("mock-maker-inline"))
    val mockAdminClient = mock[BigtableTableAdminClient]
    val kvStoreWithMocks = new BigTableKVStoreImpl(mockDataClient, mockAdminClient)
    val serverStreamingCallable = mock[ServerStreamingCallable[Query, Row]]
    val unaryCallable = mock[UnaryCallable[Query, util.List[Row]]]

    val dataset = "models"
    val key1 = "alice"
    val key2 = "bob"
    val getReq1 = GetRequest(key1.getBytes, dataset, None, None)
    val getReq2 = GetRequest(key2.getBytes, dataset, None, None)

    when(mockDataClient.readRowsCallable()).thenReturn(serverStreamingCallable)
    when(serverStreamingCallable.all()).thenReturn(unaryCallable)
    val failedFuture =
      ApiFutures.immediateFailedFuture[util.List[Row]](new RuntimeException("some BT exception on read"))
    when(unaryCallable.futureCall(any[Query])).thenReturn(failedFuture)

    val getResult = Await.result(kvStoreWithMocks.multiGet(Seq(getReq1, getReq2)), 1.second)

    getResult.size shouldBe 2
    getResult.foreach { r =>
      r.values.isFailure shouldBe true
    }
    getResult.map(v => new String(v.request.keyBytes, StandardCharsets.UTF_8)).toSet shouldBe Set(key1, key2)
  }

  // Test write and query of a simple time series dataset
  it should "time series query_multiple days" in {
    val dataset = "TILE_SUMMARIES"
    val kvStore = new BigTableKVStoreImpl(dataClient, adminClient)
    kvStore.create(dataset)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    writeGeneratedTimeSeriesData(kvStore, dataset, "my_key", tsRange, fakePayload)

    // query in time range: 10/05/24 00:00 to 10/10
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728518400000L
    val getRequest1 = GetRequest("my_key".getBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    val expectedTimeSeriesPoints = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTimeSeriesPoints, fakePayload)
  }

  it should "multiple dataset time series query_one day" in {
    val dataset = "TILE_SUMMARIES"
    val kvStore = new BigTableKVStoreImpl(dataClient, adminClient)
    kvStore.create(dataset)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    writeGeneratedTimeSeriesData(kvStore, dataset, "my_key", tsRange, fakePayload)

    // query in time range: 10/05/24 00:00 to 10/06/24 00:00
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728172800000L
    val getRequest1 = GetRequest("my_key".getBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    val expectedTimeSeriesPoints = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTimeSeriesPoints, fakePayload)
  }

  it should "multiple dataset time series query_same day" in {
    val dataset = "TILE_SUMMARIES"
    val kvStore = new BigTableKVStoreImpl(dataClient, adminClient)
    kvStore.create(dataset)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    writeGeneratedTimeSeriesData(kvStore, dataset, "my_key", tsRange, fakePayload)

    // query in time range: 10/05/24 00:00 to 10/05/24 22:20
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728166800000L
    val getRequest1 = GetRequest("my_key".getBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    val expectedTimeSeriesPoints = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTimeSeriesPoints, fakePayload)
  }

  it should "multiple dataset time series query_days without data" in {
    val dataset = "TILE_SUMMARIES"
    val kvStore = new BigTableKVStoreImpl(dataClient, adminClient)
    kvStore.create(dataset)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature": "123""""
    val dataStartTs = 1728000000000L
    val dataEndTs = 1729036800000L
    val tsRange = (dataStartTs until dataEndTs by 1.hour.toMillis)
    writeGeneratedTimeSeriesData(kvStore, dataset, "my_key", tsRange, fakePayload)

    // query in time range: 10/15/24 00:00 to 10/30/24 00:00
    val queryStartsTs = 1728950400000L
    val queryEndTs = 1730246400000L
    val getRequest1 = GetRequest("my_key".getBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    // we expect results to only cover the time range where we have data
    val expectedTimeSeriesPoints = (queryStartsTs until dataEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTimeSeriesPoints, fakePayload)
  }

  private def writeGeneratedTimeSeriesData(kvStore: BigTableKVStoreImpl,
                                           dataset: String,
                                           key: String,
                                           tsRange: Seq[Long],
                                           payload: String): Unit = {
    val points = Seq.fill(tsRange.size)(payload)
    val putRequests = tsRange.zip(points).map {
      case (ts, point) =>
        PutRequest(key.getBytes, point.getBytes, dataset, Some(ts))
    }

    val putResult = Await.result(kvStore.multiPut(putRequests), 1.second)
    putResult.length shouldBe tsRange.length
    putResult.foreach(r => r shouldBe true)
  }

  private def validateBlobValueExpectedPayload(response: GetResponse, expectedPayload: String): Unit = {
    for (
      tSeq <- response.values;
      tv <- tSeq
    ) {
      tSeq.length shouldBe 1
      val jsonStr = new String(tv.bytes, StandardCharsets.UTF_8)
      jsonStr shouldBe expectedPayload
    }
  }

  private def validateTimeSeriesValueExpectedPayload(response: GetResponse,
                                                     expectedTimestamps: Seq[Long],
                                                     expectedPayload: String): Unit = {
    for (tSeq <- response.values) {
      tSeq.map(_.millis).toSet shouldBe expectedTimestamps.toSet
      tSeq.map(v => new String(v.bytes, StandardCharsets.UTF_8)).foreach(v => v shouldBe expectedPayload)
      tSeq.length shouldBe expectedTimestamps.length
    }
  }
}
