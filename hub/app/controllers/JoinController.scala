package controllers

import io.circe.generic.auto._
import io.circe.syntax._
import model.ListJoinResponse
import play.api.mvc._
import store.MonitoringModelStore

import javax.inject._

/**
  * Controller for the Zipline Join entities
  */
@Singleton
class JoinController @Inject() (val controllerComponents: ControllerComponents, monitoringStore: MonitoringModelStore)
    extends BaseController
    with Paginate {

  /**
    * Powers the /api/v1/joins endpoint. Returns a list of models
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
        val joins = monitoringStore.getJoins
        val paginatedResults = paginateResults(joins, offsetValue, limitValue)
        val json = ListJoinResponse(offsetValue, paginatedResults).asJson.noSpaces
        Ok(json)
      }
    }

  /**
    * Returns a specific join by name
    */
  def get(name: String): Action[AnyContent] = {
    Action { implicit request: Request[AnyContent] =>
      val maybeJoin = monitoringStore.getJoins.find(j => j.name.equalsIgnoreCase(name))
      maybeJoin match {
        case None       => NotFound(s"Join: $name wasn't found")
        case Some(join) => Ok(join.asJson.noSpaces)
      }
    }
  }
}
