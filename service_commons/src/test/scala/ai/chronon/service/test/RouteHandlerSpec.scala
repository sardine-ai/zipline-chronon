package ai.chronon.service.test

import ai.chronon.api.{TimeUnit, Window}
import ai.chronon.fetcher.TileKey
import ai.chronon.observability.TileKey
import ai.chronon.service.RouteHandlerWrapper
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.ext.web.Router
import io.vertx.ext.web.client.{WebClient, WebClientOptions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterEach

import java.util.function.Function
import scala.jdk.CollectionConverters._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class RouteHandlerSpec extends AnyFlatSpec with Matchers with ScalaFutures with BeforeAndAfterEach {

  private var vertx: Vertx = _
  private var client: WebClient = _

  override def beforeEach(): Unit = {
    vertx = Vertx.vertx()
    val testPort = 8888
    client = WebClient.create(vertx, new WebClientOptions().setDefaultPort(testPort))
    val router = Router.router(vertx)

    // Create handler for pojo
    val transformer: Function[TestInput, TestOutput] = (input: TestInput) => {
      new TestOutput(
        input.getUserId,
        s"Status: ${input.getStatus}, Role: ${input.getRole}, Accuracy: ${input.getAccuracy}, " +
          s"Limit: ${input.getLimit}, Amount: ${input.getAmount}, Active: ${input.isActive}, " +
          s"Items: ${if (input.getItems != null) input.getItems.asScala.mkString(",") else "null"}, " +
          s"Props: ${if (input.getProps != null) {
              input.getProps.asScala.map { case (k, v) => s"$k:$v" }.mkString(",")
            } else "null"}"
      )
    }

    // Create handler for thrift
    val thriftTransformer: Function[TileKey, TileKey] = k => k

    // Create handler for thrift with enum inside
    val windowTransformer: Function[Window, Window] = w => w

    // routes
    router
      .get("/api/column/:column/slice/:slice")
      .handler(RouteHandlerWrapper.createHandler(thriftTransformer, classOf[TileKey]))

    router
      .get("/api/users/:userId/test")
      .handler(RouteHandlerWrapper.createHandler(transformer, classOf[TestInput]))

    router
      .get("/api/window/units/:timeUnit/")
      .handler(RouteHandlerWrapper.createHandler(windowTransformer, classOf[Window]))

    // Start server
    val startServerPromise = Promise[Unit]()
    vertx
      .createHttpServer()
      .requestHandler(router)
      .listen(testPort)

    // Wait for server to start
    whenReady(startServerPromise.future) { _ => }
  }

  override def afterEach(): Unit = {
    val closePromise = Promise[Unit]()
    vertx.close()
    whenReady(closePromise.future) { _ => }
  }

  // Helper method to wrap HTTP requests in futures
  private def sendRequest(request: WebClient => io.vertx.ext.web.client.HttpRequest[io.vertx.core.buffer.Buffer])
      : Future[io.vertx.ext.web.client.HttpResponse[io.vertx.core.buffer.Buffer]] = {
    val promise = Promise[io.vertx.ext.web.client.HttpResponse[io.vertx.core.buffer.Buffer]]()
    request(client).send()
    promise.future
  }

  "RouteHandlerWrapper" should "handle enum parameters correctly" in {
    val responseFuture = sendRequest { client =>
      client
        .get("/api/users/123/test")
        .addQueryParam("status", "PENDING")
        .addQueryParam("role", "ADMIN")
        .addQueryParam("limit", "10")
        .addQueryParam("amount", "99.99")
        .addQueryParam("active", "true")
        .addQueryParam("items", "a,b,c")
        .addQueryParam("props", "k1:v1,k2:v2,k3:v3")
    }

    whenReady(responseFuture) { response =>
      response.statusCode() shouldBe 200

      val result = response.bodyAsJsonObject()
      val summary = result.getString("summary")
      summary should include("Status: PENDING")
      summary should include("Role: ADMIN")
      summary should include("Items: a,b,c")
      summary should include("Props: k1:v1,k2:v2,k3:v3")
    }
  }

  it should "return error for invalid enum values" in {
    val responseFuture = sendRequest { client =>
      client
        .get("/api/users/123/test")
        .addQueryParam("status", "INVALID_STATUS")
    }

    whenReady(responseFuture) { response =>
      response.statusCode() shouldBe 400
      val error = response.bodyAsJsonObject()
      error.getString("error") should include("Invalid enum value")
    }
  }

  it should "handle case-insensitive enum values" in {
    val responseFuture = sendRequest { client =>
      client
        .get("/api/users/123/test")
        .addQueryParam("status", "pending") // lowercase
        .addQueryParam("role", "admin") // lowercase
        .addQueryParam("accuracy", "temporal")
    }

    whenReady(responseFuture) { response =>
      response.statusCode() shouldBe 200

      val result = response.bodyAsJsonObject()
      val summary = result.getString("summary")
      summary should include("Status: PENDING")
      summary should include("Role: ADMIN")
      summary should include("Accuracy: TEMPORAL")
    }
  }

  it should "map parameters successfully" in {
    val responseFuture = sendRequest { client =>
      client
        .get("/api/users/123/test")
        .addQueryParam("status", "pending")
        .addQueryParam("limit", "10")
        .addQueryParam("amount", "99.99")
        .addQueryParam("active", "true")
    }

    whenReady(responseFuture) { response =>
      response.statusCode() shouldBe 200

      val result = response.bodyAsJsonObject()
      result.getString("userId") shouldBe "123"
      val summary = result.getString("summary")
      summary should include("Status: PENDING")
      summary should include("Limit: 10")
      summary should include("Amount: 99.99")
      summary should include("Active: true")
    }
  }

  it should "return error for invalid number parameters" in {
    val responseFuture = sendRequest { client =>
      client
        .get("/api/users/123/test")
        .addQueryParam("limit", "not-a-number")
    }

    whenReady(responseFuture) { response =>
      response.statusCode() shouldBe 400

      val error = response.bodyAsJsonObject()
      error.getString("error") should include("not-a-number")
    }
  }

  it should "handle missing optional parameters" in {
    val responseFuture = sendRequest { client =>
      client.get("/api/users/123/test")
    }

    whenReady(responseFuture) { response =>
      response.statusCode() shouldBe 200

      val result = response.bodyAsJsonObject()
      result.getString("userId") shouldBe "123"
      val summary = result.getString("summary")
      summary should include("Status: null")
      summary should include("Limit: 0")
      summary should include("Amount: 0.00")
      summary should include("Active: false")
    }
  }

  it should "handle all parameter types" in {
    val responseFuture = sendRequest { client =>
      client
        .get("/api/users/123/test")
        .addQueryParam("status", "processing")
        .addQueryParam("limit", "5")
        .addQueryParam("amount", "123.45")
        .addQueryParam("active", "true")
    }

    whenReady(responseFuture) { response =>
      if (response.statusCode() != 200) {
        println(response.bodyAsString())
      }
      response.statusCode() shouldBe 200

      val result = response.bodyAsJsonObject()
      val summary = result.getString("summary")

      summary should include("Status: PROCESSING")
      summary should include("Limit: 5")
      summary should include("Amount: 123.45")
      summary should include("Active: true")
    }
  }

  it should "handle thrift parameters" in {
    val responseFuture = sendRequest { client =>
      client
        .get("/api/column/my_col/slice/my_slice")
        .addQueryParam("name", "my_name")
        .addQueryParam("sizeMillis", "5")
    }

    whenReady(responseFuture) { response =>
      if (response.statusCode() != 200) {
        println(response.bodyAsString())
      }
      response.statusCode() shouldBe 200

      val result = response.bodyAsJsonObject()

      result.getString("column") shouldBe "my_col"
      result.getString("slice") shouldBe "my_slice"
      result.getString("name") shouldBe "my_name"
      result.getString("sizeMillis") shouldBe "5"
    }
  }

  it should "handle thrift with enum parameters" in {
    val responseFuture = sendRequest { client =>
      client
        .get("/api/window/units/hours/")
        .addQueryParam("length", "100")
    }

    whenReady(responseFuture) { response =>
      if (response.statusCode() != 200) {
        println(response.bodyAsString())
      }
      response.statusCode() shouldBe 200

      val result = response.bodyAsJsonObject()

      result.getString("timeUnit") shouldBe "HOURS"
      result.getString("length") shouldBe "100"
    }
  }

  it should "handle serialized thrift with enum parameters" in {
    val responseFuture = sendRequest { client =>
      client
        .get("/api/window/units/hours/")
        .addQueryParam("length", "100")
        .putHeader(RouteHandlerWrapper.RESPONSE_CONTENT_TYPE_HEADER, RouteHandlerWrapper.TBINARY_B64_TYPE_VALUE)
    }

    whenReady(responseFuture) { response =>
      if (response.statusCode() != 200) {
        println(response.bodyAsString())
      }
      response.statusCode() shouldBe 200

      val payload = response.bodyAsString()
      val w = RouteHandlerWrapper.deserializeTBinaryBase64(payload, classOf[Window]).asInstanceOf[Window]

      w.timeUnit shouldBe TimeUnit.HOURS
      w.length shouldBe 100
    }
  }
}
