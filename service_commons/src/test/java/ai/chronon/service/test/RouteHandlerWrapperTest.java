package ai.chronon.service.test;

import ai.chronon.observability.TileKey;
import ai.chronon.api.TimeUnit;
import ai.chronon.api.Window;
import ai.chronon.orchestration.Conf;
import ai.chronon.orchestration.UploadRequest;
import ai.chronon.service.RouteHandlerWrapper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

// Test output class
class TestOutput {
    private final String userId;
    private final String summary;

    public TestOutput(String userId, String summary) {
        this.userId = userId;
        this.summary = summary;
    }

    public String getUserId() { return userId; }
    public String getSummary() { return summary; }
}

@ExtendWith(VertxExtension.class)
class RouteHandlerWrapperTest {

    private WebClient client;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        int testPort = 9999;
        client = WebClient.create(vertx, new WebClientOptions().setDefaultPort(testPort));
        Router router = Router.router(vertx);

        // Create handler for pojo
        Function<TestInput, TestOutput> transformer = input ->
                new TestOutput(
                        input.getUserId(),
                        String.format("Status: %s, Role: %s, Accuracy: %s, Limit: %d, Amount: %.2f, Active: %b, Items: %s, Props: %s",
                                input.getStatus(),
                                input.getRole(),
                                input.getAccuracy(),
                                input.getLimit(),
                                input.getAmount(),
                                input.isActive(),
                                input.getItems() != null ? String.join(",", input.getItems()) : "null",
                                input.getProps() != null ?
                                        input.getProps().entrySet()
                                                .stream()
                                                .map(entry -> entry.getKey() + ":" + entry.getValue())
                                                .collect(Collectors.joining(","))
                                        : "null"
                        )
                );

        // Create handler for thrift
        Function<TileKey, TileKey> thriftTransformer = input -> input;

        // Create handler for thrift with enum inside
        Function<Window, Window> windowTransformer = input -> input;

        // Create handler for nested thrift objects
        Function<UploadRequest, UploadRequest> uploadTransformer = input -> input;

        router.route().handler(BodyHandler.create());

        // routes
        router.get("/api/column/:column/slice/:slice")
                .handler(RouteHandlerWrapper.createHandler(thriftTransformer, TileKey.class));

        router.post("/api/users/:userId/test")
                .handler(RouteHandlerWrapper.createHandler(transformer, TestInput.class));

        router.get("/api/window/units/:timeUnit/")
                .handler(RouteHandlerWrapper.createHandler(windowTransformer, Window.class));

        router.post("/api/upload/:branch")
                .handler(RouteHandlerWrapper.createHandler(uploadTransformer, UploadRequest.class));

        // Start server
        vertx.createHttpServer()
                .requestHandler(router)
                .listen(testPort)
                .onComplete(testContext.succeeding(server -> testContext.completeNow()));
    }

    @AfterEach
    void tearDown(Vertx vertx, VertxTestContext testContext) {
        vertx.close().onComplete(testContext.succeeding(v -> testContext.completeNow()));
    }

    @Test
    public void testEnumParameters(VertxTestContext testContext) {
        client.post("/api/users/123/test")
                .addQueryParam("status", "PENDING")
                .addQueryParam("role", "ADMIN")
                .addQueryParam("limit", "10")
                .addQueryParam("amount", "99.99")
                .addQueryParam("active", "true")
                .sendJsonObject(new JsonObject()
                        .put("items", new JsonArray().add("a").add("b").add("c"))
                        .put("props", new JsonObject().put("k1", "v1").put("k2", "v2").put("k3", "v3")))
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(200, response.statusCode());

                    JsonObject result = response.bodyAsJsonObject();
                    String summary = result.getString("summary");
                    assertTrue(summary.contains("Status: PENDING"));
                    assertTrue(summary.contains("Role: ADMIN"));
                    assertTrue(summary.contains("Items: a,b,c"));
                    assertTrue(summary.contains("Props: k1:v1,k2:v2,k3:v3"));
                    testContext.completeNow();
                })));
    }

    @Test
    public void testInvalidEnumValue(VertxTestContext testContext) {
        client.post("/api/users/123/test")
                .addQueryParam("status", "INVALID_STATUS")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(500, response.statusCode());
                    testContext.completeNow();
                })));
    }

    @Test
    void testSuccessfulParameterMapping(VertxTestContext testContext) {
        client.post("/api/users/123/test")
                .addQueryParam("status", "PENDING")
                .addQueryParam("limit", "10")
                .addQueryParam("amount", "99.99")
                .addQueryParam("active", "true")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(200, response.statusCode());

                    JsonObject result = response.bodyAsJsonObject();
                    assertEquals("123", result.getString("userId"));
                    assertTrue(result.getString("summary").contains("Status: PENDING"));
                    assertTrue(result.getString("summary").contains("Limit: 10"));
                    assertTrue(result.getString("summary").contains("Amount: 99.99"));
                    assertTrue(result.getString("summary").contains("Active: true"));

                    testContext.completeNow();
                })));
    }

    @Test
    void testInvalidNumberParameter(VertxTestContext testContext) {
        client.post("/api/users/123/test")
                .addQueryParam("limit", "not-a-number")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(500, response.statusCode());
                    testContext.completeNow();
                })));
    }

    @Test
    void testMissingOptionalParameters(VertxTestContext testContext) {
        client.post("/api/users/123/test")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(200, response.statusCode());

                    JsonObject result = response.bodyAsJsonObject();
                    assertEquals("123", result.getString("userId"));
                    assertTrue(result.getString("summary").contains("Status: null"));
                    assertTrue(result.getString("summary").contains("Limit: 0"));
                    assertTrue(result.getString("summary").contains("Amount: 0.00"));
                    assertTrue(result.getString("summary").contains("Active: false"));

                    testContext.completeNow();
                })));
    }

    @Test
    void testAllParameterTypes(VertxTestContext testContext) {
        client.post("/api/users/123/test")
                .addQueryParam("status", "PROCESSING")
                .addQueryParam("limit", "5")
                .addQueryParam("amount", "123.45")
                .addQueryParam("active", "true")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    if(response.statusCode() != 200) {
                        System.out.println(response.bodyAsString());
                    }
                    assertEquals(200, response.statusCode());

                    JsonObject result = response.bodyAsJsonObject();
                    String summary = result.getString("summary");

                    assertTrue(summary.contains("Status: PROCESSING"));
                    assertTrue(summary.contains("Limit: 5"));
                    assertTrue(summary.contains("Amount: 123.45"));
                    assertTrue(summary.contains("Active: true"));

                    testContext.completeNow();
                })));
    }

    @Test
    void testAllParameterTypesThrift(VertxTestContext testContext) {
        client.get("/api/column/my_col/slice/my_slice")
                .addQueryParam("name", "my_name")
                .addQueryParam("sizeMillis", "5")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    if(response.statusCode() != 200) {
                        System.out.println(response.bodyAsString());
                    }
                    assertEquals(200, response.statusCode());

                    JsonObject result = response.bodyAsJsonObject();

                    assertEquals(result.getString("column"), "my_col");
                    assertEquals(result.getString("slice"), "my_slice");
                    assertEquals(result.getString("name"), "my_name");
                    assertEquals(result.getString("sizeMillis"), "5");
                    testContext.completeNow();
                })));
    }

    @Test
    void testAllParameterTypesThriftWithEnum(VertxTestContext testContext) {
        client.get("/api/window/units/HOURS/")
                .addQueryParam("length", "100")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    if(response.statusCode() != 200) {
                        System.out.println(response.bodyAsString());
                    }
                    assertEquals(200, response.statusCode());

                    JsonObject result = response.bodyAsJsonObject();

                    // Thrift enums are serialized as integer values by TSimpleJSONProtocol
                    assertEquals(String.valueOf(TimeUnit.HOURS.getValue()), result.getString("timeUnit"));
                    assertEquals("100", result.getString("length"));
                    testContext.completeNow();
                })));
    }

    @Test
    void testAllParameterTypesThriftWithEnumSerialized(VertxTestContext testContext) {
        client.get("/api/window/units/HOURS/")
                .addQueryParam("length", "100")
                .putHeader(RouteHandlerWrapper.RESPONSE_CONTENT_TYPE_HEADER,
                        RouteHandlerWrapper.TBINARY_B64_TYPE_VALUE)
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    if(response.statusCode() != 200) {
                        System.out.println(response.bodyAsString());
                    }
                    assertEquals(200, response.statusCode());

                    String payload = response.bodyAsString();
                    Window w = (Window) RouteHandlerWrapper.deserializeTBinaryBase64(payload, Window.class);

                    assertEquals(w.timeUnit, TimeUnit.HOURS);
                    assertEquals(w.length, 100);
                    testContext.completeNow();
                })));
    }

    @Test
    void testNestedThriftObject(VertxTestContext testContext) {
        // Create a JSON payload with nested objects
        JsonObject confObject1 = new JsonObject()
                .put("name", "testConfig1")
                .put("hash", "abc123")
                .put("contents", "config contents 1");

        JsonObject confObject2 = new JsonObject()
                .put("name", "testConfig2")
                .put("hash", "def456")
                .put("contents", "config contents 2");

        JsonObject requestBody = new JsonObject()
                .put("diffConfs", new io.vertx.core.json.JsonArray()
                        .add(confObject1)
                        .add(confObject2));

        client.post("/api/upload/feature-branch")
                .putHeader("Content-Type", "application/json")
                .sendJson(requestBody)
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    if(response.statusCode() != 200) {
                        System.out.println(response.bodyAsString());
                    }
                    assertEquals(200, response.statusCode());

                    JsonObject result = response.bodyAsJsonObject();
                    JsonArray diffConfs = result.getJsonArray("diffConfs");
                    assertEquals(2, diffConfs.size());
                    assertEquals("testConfig1", ((JsonObject) diffConfs.getValue(0)).getString("name"));
                    assertEquals("feature-branch", result.getString("branch"));

                    diffConfs.forEach(item -> {
                        JsonObject conf = (JsonObject) item;
                        assertNotNull(conf.getString("name"));
                        assertNotNull(conf.getString("hash"));
                        assertNotNull(conf.getString("contents"));
                    });

                    testContext.completeNow();
                })));
    }
}
