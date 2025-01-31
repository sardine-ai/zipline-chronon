package ai.chronon.hub.handlers

import ai.chronon.api.GroupBy
import ai.chronon.api.Join
import ai.chronon.api.MetaData
import ai.chronon.api.Model
import ai.chronon.api.StagingQuery
import ai.chronon.hub.ConfListRequest
import ai.chronon.hub.ConfRequest
import ai.chronon.hub.ConfType
import ai.chronon.hub.store.LoadedConfs
import ai.chronon.hub.store.MonitoringModelStore
import ai.chronon.online.TTLCache
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mock
import org.mockito.Mockito.when
import org.mockito.MockitoAnnotations

@RunWith(classOf[VertxUnitRunner])
class ConfHandlerTest {

  @Mock private var mockedStore: MonitoringModelStore = _
  @Mock private var mockedCache: TTLCache[String, LoadedConfs] = _
  private var handler: ConfHandler = _
  private val defaultRegistry = MockConfigRegistry.createMockRegistry()

  @Before
  def setUp(): Unit = {
    MockitoAnnotations.openMocks(this)
    handler = new ConfHandler(mockedStore)
    
    when(mockedStore.configRegistryCache).thenReturn(mockedCache)
    when(mockedCache.apply("default")).thenReturn(LoadedConfs(
      joins = defaultRegistry.joins,
      groupBys = defaultRegistry.groupBys,
      stagingQueries = defaultRegistry.stagingQueries,
      models = defaultRegistry.models
    ))
  }

  @Test
  def testGetConfForJoin(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.JOIN)
    request.setConfName("test_join_1")

    val result = handler.getConf(request)
    
    assertNotNull(result.getJoin)
    assertEquals("test_join_1", result.getJoin.getMetaData.getName)
  }

  @Test
  def testGetConfForModel(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.MODEL)
    request.setConfName("test_model_1")

    val result = handler.getConf(request)
    
    assertNotNull(result.getModel)
    assertEquals("test_model_1", result.getModel.getMetaData.getName)
  }

  @Test
  def testGetConfForGroupBy(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.GROUP_BY)
    request.setConfName("test_groupby_1")

    val result = handler.getConf(request)
    
    assertNotNull(result.getGroupBy)
    assertEquals("test_groupby_1", result.getGroupBy.getMetaData.getName)
  }

  @Test
  def testGetConfForStagingQuery(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.STAGING_QUERY)
    request.setConfName("test_query_1")

    val result = handler.getConf(request)
    
    assertNotNull(result.getStagingQuery)
    assertEquals("test_query_1", result.getStagingQuery.getMetaData.getName)
  }

  @Test(expected = classOf[RuntimeException])
  def testGetConfForNonexistentJoin(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.JOIN)
    request.setConfName("nonexistent_join")
    handler.getConf(request)
  }

  @Test(expected = classOf[RuntimeException])
  def testGetConfForNonexistentModel(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.MODEL)
    request.setConfName("nonexistent_model")
    handler.getConf(request)
  }

  @Test(expected = classOf[RuntimeException])
  def testGetConfForNonexistentGroupBy(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.GROUP_BY)
    request.setConfName("nonexistent_groupby")
    handler.getConf(request)
  }

  @Test(expected = classOf[RuntimeException])
  def testGetConfForNonexistentStagingQuery(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.STAGING_QUERY)
    request.setConfName("nonexistent_query")
    handler.getConf(request)
  }

  @Test
  def testGetConfListForJoins(): Unit = {
    val request = new ConfListRequest()
    request.setConfType(ConfType.JOIN)
    val result = handler.getConfList(request)
    assertNotNull(result.getJoins)
    assertEquals(3, result.getJoins.size())
  }

  @Test
  def testGetConfListForModels(): Unit = {
    val request = new ConfListRequest()
    request.setConfType(ConfType.MODEL)
    val result = handler.getConfList(request)
    assertNotNull(result.getModels)
    assertEquals(2, result.getModels.size())
  }

  @Test
  def testGetConfListForGroupBys(): Unit = {
    val request = new ConfListRequest()
    request.setConfType(ConfType.GROUP_BY)
    val result = handler.getConfList(request)
    assertNotNull(result.getGroupBys)
    assertEquals(2, result.getGroupBys.size())
  }

  @Test
  def testGetConfListForStagingQueries(): Unit = {
    val request = new ConfListRequest()
    request.setConfType(ConfType.STAGING_QUERY)
    val result = handler.getConfList(request)
    assertNotNull(result.getStagingQueries)
    assertEquals(2, result.getStagingQueries.size())
  }

  @Test
  def testSearchConfForJoins(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.JOIN)
    request.setConfName("1")
    val result = handler.searchConf(request)
    assertNotNull(result.getJoins)
    assertEquals(1, result.getJoins.size())
    assertEquals("test_join_1", result.getJoins.get(0).getMetaData.getName)
  }

  @Test
  def testSearchConfForModels(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.MODEL)
    request.setConfName("1")
    val result = handler.searchConf(request)
    assertNotNull(result.getModels)
    assertEquals(1, result.getModels.size())
    assertEquals("test_model_1", result.getModels.get(0).getMetaData.getName)
  }

  @Test
  def testSearchConfForGroupBys(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.GROUP_BY)
    request.setConfName("1")
    val result = handler.searchConf(request)
    assertNotNull(result.getGroupBys)
    assertEquals(1, result.getGroupBys.size())
    assertEquals("test_groupby_1", result.getGroupBys.get(0).getMetaData.getName)
  }

  @Test
  def testSearchConfForStagingQueries(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.STAGING_QUERY)
    request.setConfName("1")
    val result = handler.searchConf(request)
    assertNotNull(result.getStagingQueries)
    assertEquals(1, result.getStagingQueries.size())
    assertEquals("test_query_1", result.getStagingQueries.get(0).getMetaData.getName)
  }

  @Test
  def testSearchConfAcrossAllTypes(): Unit = {
    val request = new ConfRequest()
    request.setConfName("1")

    val result = handler.searchConf(request)
    
    assertEquals(1, result.getJoins.size())
    assertEquals(1, result.getModels.size())
    assertEquals(1, result.getGroupBys.size())
    assertEquals(1, result.getStagingQueries.size())
  }

  @Test
  def testSearchConfWithPartialName(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.JOIN)
    request.setConfName("join")

    val result = handler.searchConf(request)
    assertNotNull(result.getJoins)
    assertEquals(3, result.getJoins.size())
    assertEquals("test_join_1", result.getJoins.get(0).getMetaData.getName)
  }

  @Test
  def testSearchConfWithEmptyString(): Unit = {
    val request = new ConfRequest()
    request.setConfType(ConfType.JOIN)
    request.setConfName("")

    val result = handler.searchConf(request)
    assertNotNull(result.getJoins)
    assertEquals(3, result.getJoins.size())
  }
}

object MockConfigRegistry {
  def createMockRegistry(): createMockRegistry = new createMockRegistry()
  class createMockRegistry() extends {
    val joins: Seq[Join] = Seq(
      createMockJoin("test_join_1"),
      createMockJoin("test_join_2"),
      createMockJoin("test_join_3")
    )
    
    val models: Seq[Model] = Seq(
      createMockModel("test_model_1"),
      createMockModel("test_model_2")
    )
    
    val groupBys: Seq[GroupBy] = Seq(
      createMockGroupBy("test_groupby_1"),
      createMockGroupBy("test_groupby_2")
    )
    
    val stagingQueries: Seq[StagingQuery] = Seq(
      createMockStagingQuery("test_query_1"),
      createMockStagingQuery("test_query_2")
    )
  }

  private def createMetaData(name: String): MetaData = {
    val metadata = new MetaData()
    metadata.setName(name)
    metadata
  }

  private def createMockJoin(name: String): Join = {
    val join = new Join()
    join.setMetaData(createMetaData(name))
    join
  }

  private def createMockModel(name: String): Model = {
    val model = new Model()
    model.setMetaData(createMetaData(name))
    model
  }

  private def createMockGroupBy(name: String): GroupBy = {
    val groupBy = new GroupBy()
    groupBy.setMetaData(createMetaData(name))
    groupBy
  }

  private def createMockStagingQuery(name: String): StagingQuery = {
    val query = new StagingQuery()
    query.setMetaData(createMetaData(name))
    query
  }
} 