package ai.chronon.hub.handlers

import ai.chronon.hub.model.Join
import ai.chronon.hub.model.SearchJoinResponse
import ai.chronon.hub.store.MonitoringModelStore
import io.circe.generic.auto._
import io.circe.syntax._
import io.vertx.core.Handler
import io.vertx.ext.web.RoutingContext

/**
  * Powers the /api/v1/search endpoint. Returns a list of joins
  * term - Search term to search for (currently we only support searching join names)
  * offset - For pagination. We skip over offset entries before returning results
  * limit - Number of elements to return
  */
class SearchHandler(monitoringStore: MonitoringModelStore) extends Handler[RoutingContext] with Paginate {

  import VertxExtensions._

  override def handle(ctx: RoutingContext): Unit = {
    val term = Option(ctx.queryParams.get("term")).getOrElse("")
    val offset = Option(ctx.queryParams.get("offset")).map(_.toInt).getOrElse(defaultOffset)
    val limit = Option(ctx.queryParams.get("limit")).map(l => math.min(l.toInt, maxLimit)).getOrElse(defaultLimit)

    if (offset < 0) {
      ctx.BadRequest("Invalid offset - expect a positive number")
    } else if (limit < 0) {
      ctx.BadRequest("Invalid limit - expect a positive number")
    } else {
      val searchResults = searchRegistry(term)
      val paginatedResults = paginateResults(searchResults, offset, limit)
      val json = SearchJoinResponse(offset, paginatedResults).asJson.noSpaces
      ctx.Ok(json)
    }
  }

  // a trivial search where we check the join name for similarity with the search term
  private def searchRegistry(term: String): Seq[Join] = {
    val joins = monitoringStore.getJoins
    joins.filter(j => j.name.contains(term))
  }
}
