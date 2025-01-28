package ai.chronon.hub.handlers

import io.vertx.ext.web.RoutingContext

object VertxExtensions {

  implicit class RoutingContextOps(ctx: RoutingContext) {
    def Ok(message: String): Unit = {
      response(200, message)
    }

    def BadRequest(message: String): Unit = {
      response(400, message)
    }

    def NotFound(message: String): Unit = {
      response(404, message)
    }

    def InternalServerError(message: String): Unit = {
      response(500, message)
    }

    private def response(statusCode: Int, message: String): Unit = {
      ctx.response().setStatusCode(statusCode).putHeader("content-type", "application/json").end(message)
    }
  }
}
