package ai.chronon.hub.handlers

import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.PutRequest
import io.circe.Codec
import io.circe.Decoder
import io.circe.Encoder
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.ext.web.RoutingContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.Base64
import scala.compat.java8.FutureConverters

class InMemKVStoreHandler(val kvStore: KVStore) extends Handler[RoutingContext] {
  import PutRequestCodec._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def handle(ctx: RoutingContext): Unit = {
    val jsonBody = Option(ctx.body.asString())
    jsonBody match {
      case Some(jsonStr) =>
        decode[Array[PutRequest]](jsonStr) match {
          case Right(putRequests) =>
            logger.debug(s"Attempting a bulkPut with ${putRequests.length} items")
            val resultFuture = kvStore
              .multiPut(putRequests)
              .map { responses =>
                if (responses.contains(false)) logger.warn("Some write failures encountered")
              }(kvStore.executionContext)

            // wrap the Java future we get in a Vert.x Future to not block the worker thread
            val vertxResultFuture =
              Future.fromCompletionStage(FutureConverters.toJava(resultFuture).toCompletableFuture)
            vertxResultFuture.onSuccess { _ =>
              ctx.response.setStatusCode(200).putHeader("content-type", "application/json").end("Success")
            }
            vertxResultFuture.onFailure { err =>
              ctx.response
                .setStatusCode(500)
                .putHeader("content-type", "application/json")
                .end(s"Write failed - ${err.getMessage}")
            }
          case Left(error) =>
            ctx.response
              .setStatusCode(400)
              .putHeader("content-type", "application/json")
              .end(s"Unable to parse - ${error.getMessage}")
        }
      case None => ctx.response.setStatusCode(200).putHeader("content-type", "application/json").end("Empty body")
    }
  }
}

object PutRequestCodec {
  // Custom codec for byte arrays using Base64
  implicit val byteArrayEncoder: Encoder[Array[Byte]] =
    Encoder.encodeString.contramap[Array[Byte]](Base64.getEncoder.encodeToString)

  implicit val byteArrayDecoder: Decoder[Array[Byte]] =
    Decoder.decodeString.map(Base64.getDecoder.decode)

  // Derive codec for PutRequest
  implicit val putRequestCodec: Codec[PutRequest] = deriveCodec[PutRequest]
}
