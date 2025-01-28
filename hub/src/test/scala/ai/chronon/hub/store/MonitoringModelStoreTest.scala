package ai.chronon.hub.store

import ai.chronon.online.Api
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.ListRequest
import ai.chronon.online.KVStore.ListResponse
import ai.chronon.online.KVStore.ListValue
import ai.chronon.online.MetadataEndPoint
import org.junit.Before
import org.junit.Test
import org.mockito.Answers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.nio.charset.StandardCharsets
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.io.Source
import scala.util.Success
import scala.util.Try

class MonitoringModelStoreTest extends MockitoSugar with Matchers {

  var api: Api = _
  var kvStore: KVStore = _

  implicit val ec: ExecutionContext = ExecutionContext.global

  @Before
  def setup(): Unit = {
    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    api = mock[Api]
    // The KVStore execution context is implicitly used for
    // Future compositions in the Fetcher so provision it in
    // the mock to prevent hanging.
    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
    when(api.genKvStore).thenReturn(kvStore)
  }

  @Test
  def monitoringStoreShouldReturnModels(): Unit = {
    val dynamoDBMonitoringStore = new MonitoringModelStore(api)
    when(kvStore.list(any())).thenReturn(generateListResponse())

    validateLoadedConfigs(dynamoDBMonitoringStore)
  }

  private def validateLoadedConfigs(dynamoDBMonitoringStore: MonitoringModelStore): Unit = {
    // check that our store has loaded the relevant artifacts
    dynamoDBMonitoringStore.getConfigRegistry.models.length shouldBe 1
    dynamoDBMonitoringStore.getConfigRegistry.groupBys.length shouldBe 2
    dynamoDBMonitoringStore.getConfigRegistry.joins.length shouldBe 1
    dynamoDBMonitoringStore.getConfigRegistry.stagingQueries.length shouldBe 0

    // let's check models specifically
    val models = dynamoDBMonitoringStore.getModels
    models.length shouldBe 1

    val firstModel = models.head
    firstModel.name shouldBe "risk.transaction_model.v1"
    firstModel.join.name shouldBe "risk.user_transactions.txn_join"
    firstModel.join.joinFeatures.isEmpty shouldBe true
    firstModel.join.groupBys.length shouldBe 4
    firstModel.join.groupBys.foreach(g => g.features.isEmpty shouldBe false)
  }

  private def generateListResponse(): Future[ListResponse] = {
    val paths = Seq(
      "joins/user_transactions.txn_join",
      "group_bys/transaction_events.txn_group_by_merchant",
      "group_bys/transaction_events.txn_group_by_user",
      "models/transaction_model.v1"
    )

    val kvStrs = paths.map { path =>
      val confResource = getClass.getResource(s"/$path")
      val src = Source.fromFile(confResource.getPath)
      (path, src.mkString)
    }

    val kvBytes = kvStrs.map(kv => (kv._1.getBytes(StandardCharsets.UTF_8), kv._2.getBytes(StandardCharsets.UTF_8)))
    val listResponseValues: Try[Seq[ListValue]] = Success(kvBytes.map(kv => ListValue(kv._1, kv._2)))
    Future(ListResponse(ListRequest(MetadataEndPoint.ConfByKeyEndPointName, Map.empty), listResponseValues, Map.empty))
  }
}
