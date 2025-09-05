package ai.chronon.spark.test.kv_store

import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api._
import ai.chronon.online.{Api, KVStore}
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.planner.{GroupByUploadToKVNode, JoinMetadataUpload, NodeContent}
import ai.chronon.spark.kv_store.KVUploadNodeRunner
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.stubbing.Stubber
import org.mockito.{Answers, ArgumentCaptor, Mockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext

trait MockitoHelper extends MockitoSugar {
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

class MockKVUploadNodeRunner(api: Api, mockFetchContext: FetchContext) extends KVUploadNodeRunner(api) {
  override def createFetchContext(): FetchContext = mockFetchContext
}

class KVUploadNodeRunnerTest
    extends AnyFlatSpec
    with MockitoSugar
    with Matchers
    with MockitoHelper
    with BeforeAndAfter {

  var api: Api = _
  var kvStore: KVStore = _
  var metadataStore: MetadataStore = _
  var fetchContext: FetchContext = _
  var runner: MockKVUploadNodeRunner = _

  before {
    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    api = mock[Api]
    fetchContext = mock[FetchContext]
    metadataStore = mock[MetadataStore]

    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
    when(api.genKvStore).thenReturn(kvStore)
    when(fetchContext.kvStore).thenReturn(kvStore)
    when(fetchContext.metadataDataset).thenReturn(MetadataDataset)

    runner = new MockKVUploadNodeRunner(api, fetchContext)
  }

  "KVUploadNodeRunner" should "handle GROUP_BY_UPLOAD_TO_KV successfully" in {
    // Setup test data
    val groupByMetadata = new MetaData()
      .setName("test_groupby")
      .setOutputNamespace("test_namespace")

    val groupBy = new GroupBy()
      .setMetaData(groupByMetadata)

    val groupByNode = new GroupByUploadToKVNode()
      .setGroupBy(groupBy)

    val nodeContent = new NodeContent()
    nodeContent.setGroupByUploadToKV(groupByNode)

    val metadata = new MetaData()
    val range = Some(new PartitionRange("2023-01-01", "2023-01-01")(PartitionSpec.daily))

    // Execute
    runner.run(metadata, nodeContent, range)

    // Verify
    val uploadTableCaptor = ArgumentCaptor.forClass(classOf[String])
    val groupByNameCaptor = ArgumentCaptor.forClass(classOf[String])
    val partitionCaptor = ArgumentCaptor.forClass(classOf[String])

    verify(kvStore).bulkPut(
      uploadTableCaptor.capture(),
      groupByNameCaptor.capture(),
      partitionCaptor.capture()
    )

    // The upload table should be derived as: outputNamespace.name_upload
    uploadTableCaptor.getValue shouldEqual "test_namespace.test_groupby__upload"
    groupByNameCaptor.getValue shouldEqual "test_groupby"
    partitionCaptor.getValue shouldEqual "2023-01-01"
  }

  it should "handle JOIN_METADATA_UPLOAD successfully" in {
    // Setup test data
    val joinMetadata = new MetaData()
      .setName("test_join")
      .setTeam("test_team")

    val join = new Join()
      .setMetaData(joinMetadata)

    val joinMetadataUpload = new JoinMetadataUpload()
      .setJoin(join)

    val nodeContent = new NodeContent()
    nodeContent.setJoinMetadataUpload(joinMetadataUpload)

    val metadata = new MetaData()
    val range = None

    // Execute - this will create a new MetadataStore internally and call putJoinConf
    runner.run(metadata, nodeContent, range)

    // Verify that fetchContext was used (indicating MetadataStore creation and usage)
    verify(fetchContext).kvStore
    // MetadataStore.putJoinConf calls metadataDataset twice, so we expect at least one call
    verify(fetchContext, times(2)).metadataDataset
  }

  it should "throw IllegalArgumentException when partition range is missing for GROUP_BY_UPLOAD_TO_KV" in {
    // Setup test data
    val groupByMetadata = new MetaData()
      .setName("test_groupby")
      .setOutputNamespace("test_namespace")

    val groupBy = new GroupBy()
      .setMetaData(groupByMetadata)

    val groupByNode = new GroupByUploadToKVNode()
      .setGroupBy(groupBy)

    val nodeContent = new NodeContent()
    nodeContent.setGroupByUploadToKV(groupByNode)

    val metadata = new MetaData()
    val range = None // Missing partition range

    // Execute and verify exception
    val exception = intercept[IllegalArgumentException] {
      runner.run(metadata, nodeContent, range)
    }

    exception.getMessage should include("PartitionRange is required for KV upload")
  }

  it should "throw IllegalArgumentException for unsupported node content types" in {
    // Setup test data with unsupported node content
    val nodeContent = new NodeContent()
    // Not setting any supported field

    val metadata = new MetaData()
    val range = None

    // Execute and verify exception
    val exception = intercept[IllegalArgumentException] {
      runner.run(metadata, nodeContent, range)
    }

    exception.getMessage should include("Expected GroupByUploadToKVNode or JoinMetadataUpload content")
  }

  it should "propagate exceptions from KVStore bulkPut" in {
    // Setup test data
    val groupByMetadata = new MetaData()
      .setName("test_groupby")
      .setOutputNamespace("test_namespace")

    val groupBy = new GroupBy()
      .setMetaData(groupByMetadata)

    val groupByNode = new GroupByUploadToKVNode()
      .setGroupBy(groupBy)

    val nodeContent = new NodeContent()
    nodeContent.setGroupByUploadToKV(groupByNode)

    val metadata = new MetaData()
    val range = Some(new PartitionRange("2023-01-01", "2023-01-01")(PartitionSpec.daily))

    // Mock kvStore to throw exception
    val testException = new RuntimeException("KVStore error")
    when(kvStore.bulkPut(any[String], any[String], any[String])).thenThrow(testException)

    // Execute and verify exception is propagated
    val exception = intercept[RuntimeException] {
      runner.run(metadata, nodeContent, range)
    }

    exception.getMessage shouldEqual "KVStore error"
  }

  it should "propagate exceptions from createFetchContext" in {
    // Setup test data
    val joinMetadata = new MetaData()
      .setName("test_join")
      .setTeam("test_team")

    val join = new Join()
      .setMetaData(joinMetadata)

    val joinMetadataUpload = new JoinMetadataUpload()
      .setJoin(join)

    val nodeContent = new NodeContent()
    nodeContent.setJoinMetadataUpload(joinMetadataUpload)

    val metadata = new MetaData()
    val range = None

    // Create a runner that will throw an exception on fetchContext creation
    val testException = new RuntimeException("FetchContext creation error")
    val failingRunner = new KVUploadNodeRunner(api) {
      override def createFetchContext(): FetchContext = throw testException
    }

    // Execute and verify exception is propagated
    val exception = intercept[RuntimeException] {
      failingRunner.run(metadata, nodeContent, range)
    }

    exception.getMessage shouldEqual "FetchContext creation error"
  }
}
