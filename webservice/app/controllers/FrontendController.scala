package controllers

import play.api.mvc._
import play.twirl.api.Html

import javax.inject._

@Singleton
class FrontendController @Inject() (val controllerComponents: ControllerComponents) extends BaseController {
  def home(): Action[AnyContent] =
    Action { implicit request: Request[AnyContent] =>
      Ok(views.html.index("Chronon Home")(Html("<h1>Welcome to the Zipline homepage!</h1>")))
    }
}
