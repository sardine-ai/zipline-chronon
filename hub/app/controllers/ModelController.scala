package controllers

import io.circe.generic.auto._
import io.circe.syntax._
import model.ListModelResponse
import play.api.mvc._
import store.DynamoDBMonitoringStore

import javax.inject._

/**
  * Controller for the Zipline models entities
  */
@Singleton
class ModelController @Inject() (val controllerComponents: ControllerComponents,
                                 monitoringStore: DynamoDBMonitoringStore)
    extends BaseController
    with Paginate {

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
        val models = monitoringStore.getModels
        val paginatedResults = paginateResults(models, offsetValue, limitValue)
        val json = ListModelResponse(offsetValue, paginatedResults).asJson.noSpaces
        Ok(json)
      }
    }
}
