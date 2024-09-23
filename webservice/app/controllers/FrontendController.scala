package controllers

import javax.inject._
import play.api.mvc._

@Singleton
class FrontendController @Inject()(val controllerComponents: ControllerComponents) extends BaseController {
  def home(): Action[AnyContent] = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.index("Welcome to the Zipline homepage!"))
  }
}
