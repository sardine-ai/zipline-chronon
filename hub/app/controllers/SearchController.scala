package controllers

import io.circe.generic.auto._
import io.circe.syntax._
import model.Model
import model.SearchModelResponse
import play.api.mvc._

import javax.inject._

/**
  * Controller to power search related APIs
  */
class SearchController @Inject() (val controllerComponents: ControllerComponents) extends BaseController {

  // temporarily serve up mock data while we wait on hooking up our KV store layer
  private[this] def generateMockModel(id: String): Model =
    Model(s"my test model - $id",
          id,
          online = true,
          production = true,
          "my team",
          "XGBoost",
          1719262147000L,
          1727210947000L)
  private[this] val mockModelRegistry: Seq[Model] = (0 until 100).map(i => generateMockModel(i.toString))

  val defaultOffset = 0
  val defaultLimit = 10

  /**
    * Powers the /api/v1/search endpoint. Returns a list of models
    * @param term - Search term to search for (currently we only support searching model names)
    * @param offset - For pagination. We skip over offset entries before returning results
    * @param limit - Number of elements to return
    */
  def search(term: String, offset: Option[Int], limit: Option[Int]): Action[AnyContent] =
    Action { implicit request: Request[AnyContent] =>
      // Default values if the parameters are not provided
      val offsetValue = offset.getOrElse(defaultOffset)
      val limitValue = limit.getOrElse(defaultLimit)

      if (offsetValue < 0) {
        BadRequest("Invalid offset - expect a positive number")
      } else if (limitValue < 0) {
        BadRequest("Invalid limit - expect a positive number")
      } else {
        val searchResults = searchRegistry(term)
        val paginatedResults = searchResults.slice(offsetValue, offsetValue + limitValue)
        val json = SearchModelResponse(offsetValue, paginatedResults).asJson.noSpaces
        Ok(json)
      }
    }

  // a trivial search where we check the model name for similarity with the search term
  private def searchRegistry(term: String): Seq[Model] = {
    mockModelRegistry.filter(m => m.name.contains(term))
  }
}
