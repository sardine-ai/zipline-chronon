package controllers

import play.api.mvc._

import javax.inject._

/**
  * Controller to serve up any backend application related items
  */
class ApplicationController @Inject() (val controllerComponents: ControllerComponents) extends BaseController {
  def ping(): Action[AnyContent] =
    Action { implicit request: Request[AnyContent] =>
      Ok("pong!")
    }
}
