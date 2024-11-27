package controllers

import io.circe.generic.auto._
import io.circe.syntax._
import model.Join
import model.SearchJoinResponse
import play.api.mvc._
import store.MonitoringModelStore

import javax.inject._

/**
  * Controller to power search related APIs
  */
class SearchController @Inject() (val controllerComponents: ControllerComponents, monitoringStore: MonitoringModelStore)
    extends BaseController
    with Paginate {

  /**
    * Powers the /api/v1/search endpoint. Returns a list of joins
    * @param term - Search term to search for (currently we only support searching join names)
    * @param offset - For pagination. We skip over offset entries before returning results
    * @param limit - Number of elements to return
    */
  def search(term: String, offset: Option[Int], limit: Option[Int]): Action[AnyContent] =
    Action { implicit request: Request[AnyContent] =>
      // Default values if the parameters are not provided
      val offsetValue = offset.getOrElse(defaultOffset)
      val limitValue = limit.map(l => math.min(l, maxLimit)).getOrElse(defaultLimit)

      if (offsetValue < 0) {
        BadRequest("Invalid offset - expect a positive number")
      } else if (limitValue < 0) {
        BadRequest("Invalid limit - expect a positive number")
      } else {
        val searchResults = searchRegistry(term)
        val paginatedResults = paginateResults(searchResults, offsetValue, limitValue)
        val json = SearchJoinResponse(offsetValue, paginatedResults).asJson.noSpaces
        Ok(json)
      }
    }

  // a trivial search where we check the join name for similarity with the search term
  private def searchRegistry(term: String): Seq[Join] = {
    val joins = monitoringStore.getJoins
    joins.filter(j => j.name.contains(term))
  }
}
