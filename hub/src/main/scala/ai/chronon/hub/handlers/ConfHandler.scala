package ai.chronon.hub.handlers

import ai.chronon.hub.ConfListRequest
import ai.chronon.hub.ConfListResponse
import ai.chronon.hub.ConfRequest
import ai.chronon.hub.ConfType
import ai.chronon.hub.store.MonitoringModelStore
import ai.chronon.orchestration.LogicalNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class ConfHandler(store: MonitoringModelStore) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Returns a specific configuration by name and type
    */
  def getConf(req: ConfRequest): LogicalNode = {
    logger.debug(s"Retrieving ${req.getConfName} of type ${req.getConfType}")
    val registry = store.configRegistryCache("default")

    val node = new LogicalNode()

    req.getConfType match {
      case ConfType.JOIN =>
        node.setJoin(findConfig(registry.joins, "join", req.getConfName))
      case ConfType.GROUP_BY =>
        node.setGroupBy(findConfig(registry.groupBys, "groupBy", req.getConfName))
      case ConfType.MODEL =>
        node.setModel(findConfig(registry.models, "model", req.getConfName))
      case ConfType.STAGING_QUERY =>
        node.setStagingQuery(findConfig(registry.stagingQueries, "staging query", req.getConfName))
      case _ => throw new RuntimeException(s"Unsupported configuration type ${req.getConfType}")
    }

    node
  }

  /**
    * Finds a specific configuration by name within a sequence of configs
    */
  private def findConfig[T](configs: Seq[T], configType: String, name: String): T = {
    configs
      .find(_.asInstanceOf[{ def getMetaData: { def getName: String } }].getMetaData.getName.equalsIgnoreCase(name))
      .getOrElse(throw new RuntimeException(s"Unable to retrieve $configType $name"))
  }

  /**
    * Returns all configurations of a specific type
    */
  def getConfList(req: ConfListRequest): ConfListResponse = {
    logger.debug(s"Retrieving all configurations of type ${req.getConfType}")
    val registry = store.configRegistryCache("default")

    val response = new ConfListResponse()

    req.getConfType match {
      case ConfType.JOIN =>
        response.setJoins(registry.joins.asJava)
      case ConfType.GROUP_BY =>
        response.setGroupBys(registry.groupBys.asJava)
      case ConfType.MODEL =>
        response.setModels(registry.models.asJava)
      case ConfType.STAGING_QUERY =>
        response.setStagingQueries(registry.stagingQueries.asJava)
      case _ => throw new RuntimeException(s"Unsupported configuration type ${req.getConfType}")
    }
    response
  }

  /**
    * Returns configurations matching the search criteria
    */
  def searchConf(req: ConfRequest): ConfListResponse = {
    logger.debug(s"Searching for configurations matching '${req.getConfName}' of type ${req.getConfType}")
    val registry = store.configRegistryCache("default")
    val searchTerm = Option(req.getConfName).getOrElse("").toLowerCase

    val response = new ConfListResponse()

    // Helper function to filter configs by name
    def filterByName[T](configs: Seq[T]): Seq[T] = {
      configs.filter(
        _.asInstanceOf[{ def getMetaData: { def getName: String } }].getMetaData.getName.toLowerCase
          .contains(searchTerm))
    }

    // If confType is specified, only search that type
    Option(req.getConfType) match {
      case Some(ConfType.JOIN) =>
        response.setJoins(filterByName(registry.joins).asJava)
      case Some(ConfType.GROUP_BY) =>
        response.setGroupBys(filterByName(registry.groupBys).asJava)
      case Some(ConfType.MODEL) =>
        response.setModels(filterByName(registry.models).asJava)
      case Some(ConfType.STAGING_QUERY) =>
        response.setStagingQueries(filterByName(registry.stagingQueries).asJava)
      case None =>
        // If no type specified, search all types
        response.setJoins(filterByName(registry.joins).asJava)
        response.setGroupBys(filterByName(registry.groupBys).asJava)
        response.setModels(filterByName(registry.models).asJava)
        response.setStagingQueries(filterByName(registry.stagingQueries).asJava)
    }

    response
  }
}
