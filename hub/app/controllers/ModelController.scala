package controllers

import io.circe.generic.auto._
import io.circe.syntax._
import model.ListModelResponse
import model.Model
import play.api.mvc._

import javax.inject._

/**
  * Controller for the Zipline models entities
  */
@Singleton
class ModelController @Inject() (val controllerComponents: ControllerComponents) extends BaseController with Paginate {

  import MockDataService._

  /**
    * Powers the /api/v1/models endpoint. Returns a list of models
    * @param offset - For pagination. We skip over offset entries before returning results
    * @param limit - Number of elements to return
    */
  def list(offset: Option[Int], limit: Option[Int]): Action[AnyContent] =
    Action { implicit request: Request[AnyContent] =>
      // Default values if the parameters are not provided
      val offsetValue = offset.getOrElse(defaultOffset)
      val limitValue = limit.map(l => math.min(l, maxLimit)).getOrElse(defaultLimit)

      if (offsetValue < 0) {
        BadRequest("Invalid offset - expect a positive number")
      } else if (limitValue < 0) {
        BadRequest("Invalid limit - expect a positive number")
      } else {
        val paginatedResults = paginateResults(mockModelRegistry, offsetValue, limitValue)
        val json = ListModelResponse(offsetValue, paginatedResults).asJson.noSpaces
        Ok(json)
      }
    }
}

object MockDataService {
  // temporarily serve up mock data while we wait on hooking up our KV store layer
  def generateMockModel(id: String): Model =
    Model(s"my test model - $id",
          id,
          online = true,
          production = true,
          "my team",
          "XGBoost",
          1719262147000L,
          1727210947000L)

  val mockModelRegistry: Seq[Model] = (0 until 100).map(i => generateMockModel(i.toString))
}
