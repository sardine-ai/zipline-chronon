package ai.chronon.hub.handlers

import ai.chronon.hub.model.ListJoinResponse
import ai.chronon.hub.store.MonitoringModelStore
import io.circe.generic.auto._
import io.circe.syntax._
import io.vertx.core.Handler
import io.vertx.ext.web.RoutingContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class JoinsHandler(monitoringStore: MonitoringModelStore) extends Paginate {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  import VertxExtensions._

  /**
    * Powers the /api/v1/joins endpoint. Returns a list of models
    * offset - For pagination. We skip over offset entries before returning results
    * limit - Number of elements to return
    */
  val listHandler: Handler[RoutingContext] = (ctx: RoutingContext) => {
    val offset = Option(ctx.queryParams.get("offset")).map(_.toInt).getOrElse(defaultOffset)
    val limit = Option(ctx.queryParams.get("limit")).map(l => math.min(l.toInt, maxLimit)).getOrElse(defaultLimit)

    if (offset < 0) {
      ctx.BadRequest("Invalid offset - expect a positive number")
    } else if (limit < 0) {
      ctx.BadRequest("Invalid limit - expect a positive number")
    } else {
      val joins = monitoringStore.getJoins
      val paginatedResults = paginateResults(joins, offset, limit)
      val json = ListJoinResponse(offset, paginatedResults).asJson.noSpaces
      ctx.Ok(json)
    }
  }

  /**
    * Returns a specific join by name (/api/v1/join/:name)
    */
  val getHandler: Handler[RoutingContext] = (ctx: RoutingContext) => {
    val entityName = ctx.pathParam("name");
    logger.debug("Retrieving {}", entityName);

    val maybeJoin = monitoringStore.getJoins.find(j => j.name.equalsIgnoreCase(entityName))
    maybeJoin match {
      case None       => ctx.NotFound(s"Unable to retrive $entityName")
      case Some(join) => ctx.Ok(join.asJson.noSpaces)
    }
  }

}
