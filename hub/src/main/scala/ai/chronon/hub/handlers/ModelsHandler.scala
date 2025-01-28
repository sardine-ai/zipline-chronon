package ai.chronon.hub.handlers

import ai.chronon.hub.model.ListModelResponse
import ai.chronon.hub.store.MonitoringModelStore
import io.circe.generic.auto._
import io.circe.syntax._
import io.vertx.core.Handler
import io.vertx.ext.web.RoutingContext

/**
  * Powers the /api/v1/models endpoint. Returns a list of models
  * offset - For pagination. We skip over offset entries before returning results
  * limit - Number of elements to return
  */
class ModelsHandler(monitoringStore: MonitoringModelStore) extends Handler[RoutingContext] with Paginate {

  import VertxExtensions._

  override def handle(ctx: RoutingContext): Unit = {
    val offset = Option(ctx.queryParams.get("offset")).map(_.toInt).getOrElse(defaultOffset)
    val limit = Option(ctx.queryParams.get("limit")).map(l => math.min(l.toInt, maxLimit)).getOrElse(defaultLimit)

    if (offset < 0) {
      ctx.BadRequest("Invalid offset - expect a positive number")
    } else if (limit < 0) {
      ctx.BadRequest("Invalid limit - expect a positive number")
    } else {
      val models = monitoringStore.getModels
      val paginatedResults = paginateResults(models, offset, limit)
      val json = ListModelResponse(offset, paginatedResults).asJson.noSpaces
      ctx.Ok(json)
    }
  }
}
