package controllers

import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.PutRequest
import io.circe.Codec
import io.circe.Decoder
import io.circe.Encoder
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import play.api.Logger
import play.api.mvc
import play.api.mvc.BaseController
import play.api.mvc.ControllerComponents
import play.api.mvc.RawBuffer

import java.util.Base64
import javax.inject.Inject
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class InMemKVStoreController @Inject() (val controllerComponents: ControllerComponents, kvStore: KVStore)(implicit
    ec: ExecutionContext)
    extends BaseController {

  import PutRequestCodec._

  val logger: Logger = Logger(this.getClass)

  def bulkPut(): mvc.Action[RawBuffer] =
    Action(parse.raw).async { request =>
      request.body.asBytes() match {
        case Some(bytes) =>
          decode[Array[PutRequest]](bytes.utf8String) match {
            case Right(putRequests) =>
              logger.debug(s"Attempting a bulkPut with ${putRequests.length} items")
              val resultFuture = kvStore.multiPut(putRequests)
              resultFuture.map { responses =>
                if (responses.contains(false)) {
                  logger.warn("Some write failures encountered")
                }
                Ok("Success")
              }
            case Left(error) => Future.successful(BadRequest(error.getMessage))
          }
        case None => Future.successful(BadRequest("Empty body"))
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
