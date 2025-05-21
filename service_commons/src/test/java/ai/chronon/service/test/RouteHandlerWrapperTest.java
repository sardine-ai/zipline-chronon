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

import java.util.concurrent.CompletableFuture;
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

    // POJO handlers for sync and async operations
    private Function<TestInput, TestOutput> createSyncTransformer() {
        return input -> new TestOutput(
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
    }
    
    private Function<TestInput, CompletableFuture<TestOutput>> createAsyncTransformer(Vertx vertx) {
        return input -> {
            CompletableFuture<TestOutput> future = new CompletableFuture<>();
            
            // Simulate async processing
            vertx.setTimer(10, id -> {
                future.complete(new TestOutput(
                        input.getUserId(),
                        String.format("Async Status: %s, Role: %s, Accuracy: %s, Limit: %d, Amount: %.2f, Active: %b, Items: %s, Props: %s",
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
                ));
            });
            
            return future;
        };
    }
    
    private Function<TestInput, CompletableFuture<TestOutput>> createFailingAsyncTransformer(Vertx vertx) {
        return input -> {
            CompletableFuture<TestOutput> future = new CompletableFuture<>();
            
            // Simulate async processing that fails
            vertx.setTimer(10, id -> {
                if ("fail".equals(input.getUserId())) {
                    future.completeExceptionally(new IllegalArgumentException("Failed to process request"));
                } else if ("error".equals(input.getUserId())) {
                    future.completeExceptionally(new RuntimeException("Internal server error"));
                } else {
                    future.complete(new TestOutput(
                            input.getUserId(),
                            "This should not be reached for fail/error user IDs"
                    ));
                }
            });
            
            return future;
        };
    }
    
    // Thrift handlers for sync and async operations
    private Function<TileKey, TileKey> createThriftTransformer() {
        return input -> input;
    }
    
    private Function<TileKey, CompletableFuture<TileKey>> createAsyncThriftTransformer(Vertx vertx) {
        return input -> {
            CompletableFuture<TileKey> future = new CompletableFuture<>();
            vertx.setTimer(10, id -> future.complete(input));
            return future;
        };
    }
    
    private Function<Window, Window> createWindowTransformer() {
        return input -> input;
    }
    
    private Function<UploadRequest, UploadRequest> createUploadTransformer() {
        return input -> input;
    }
    
    private void configureRoutes(Router router, Vertx vertx) {
        // Create all transformer functions
        Function<TestInput, TestOutput> transformer = createSyncTransformer();
        Function<TestInput, CompletableFuture<TestOutput>> asyncTransformer = createAsyncTransformer(vertx);
        Function<TestInput, CompletableFuture<TestOutput>> failingAsyncTransformer = createFailingAsyncTransformer(vertx);
        Function<TileKey, TileKey> thriftTransformer = createThriftTransformer();
        Function<TileKey, CompletableFuture<TileKey>> asyncThriftTransformer = createAsyncThriftTransformer(vertx);
        Function<Window, Window> windowTransformer = createWindowTransformer();
        Function<UploadRequest, UploadRequest> uploadTransformer = createUploadTransformer();
        
        router.route().handler(BodyHandler.create());

        // Regular synchronous routes
        router.get("/api/column/:column/slice/:slice")
                .handler(RouteHandlerWrapper.createHandler(thriftTransformer, TileKey.class));
        router.post("/api/users/:userId/test")
                .handler(RouteHandlerWrapper.createHandler(transformer, TestInput.class));
        router.get("/api/window/units/:timeUnit/")
                .handler(RouteHandlerWrapper.createHandler(windowTransformer, Window.class));
        router.post("/api/upload/:branch")
                .handler(RouteHandlerWrapper.createHandler(uploadTransformer, UploadRequest.class));
                
        // Async routes
        router.post("/api/users/:userId/async-test")
                .handler(RouteHandlerWrapper.createAsyncHandler(asyncTransformer, TestInput.class));
        router.post("/api/users/:userId/async-test-failing")
                .handler(RouteHandlerWrapper.createAsyncHandler(failingAsyncTransformer, TestInput.class));
        router.get("/api/async-column/:column/slice/:slice")
                .handler(RouteHandlerWrapper.createAsyncHandler(asyncThriftTransformer, TileKey.class));
    }
    
    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        final int testPort = 9999;
        client = WebClient.create(vertx, new WebClientOptions().setDefaultPort(testPort));
        Router router = Router.router(vertx);

        // Configure all routes with appropriate handlers
        configureRoutes(router, vertx);

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
    
    @Test
    void testAsyncHandlerSuccess(VertxTestContext testContext) {
        client.post("/api/users/456/async-test")
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

                    assertTrue(summary.contains("Async Status: PROCESSING"));
                    assertTrue(summary.contains("Limit: 5"));
                    assertTrue(summary.contains("Amount: 123.45"));
                    assertTrue(summary.contains("Active: true"));
                    assertEquals("456", result.getString("userId"));

                    testContext.completeNow();
                })));
    }
    
    @Test
    void testAsyncHandlerJsonWithParams(VertxTestContext testContext) {
        client.post("/api/users/789/async-test")
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
                    assertTrue(summary.contains("Async Status: PENDING"));
                    assertTrue(summary.contains("Role: ADMIN"));
                    assertTrue(summary.contains("Items: a,b,c"));
                    assertTrue(summary.contains("Props: k1:v1,k2:v2,k3:v3"));
                    assertEquals("789", result.getString("userId"));
                    
                    testContext.completeNow();
                })));
    }
    
    @Test
    void testAsyncHandlerError(VertxTestContext testContext) {
        client.post("/api/users/error/async-test-failing")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(500, response.statusCode());
                    String errorPayload = response.bodyAsString();
                    assertTrue(errorPayload.contains("Internal server error"));
                    testContext.completeNow();
                })));
    }
    
    @Test
    void testAsyncHandlerIllegalArgument(VertxTestContext testContext) {
        client.post("/api/users/fail/async-test-failing")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(400, response.statusCode());
                    String errorPayload = response.bodyAsString();
                    assertTrue(errorPayload.contains("Failed to process request"));
                    testContext.completeNow();
                })));
    }
    
    @Test
    void testAsyncHandlerInvalidInput(VertxTestContext testContext) {
        client.post("/api/users/123/async-test")
                .addQueryParam("limit", "not-a-number")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(500, response.statusCode());
                    testContext.completeNow();
                })));
    }
    
    @Test
    void testAsyncHandlerThrift(VertxTestContext testContext) {
        client.get("/api/async-column/my_col/slice/my_slice")
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
    void testAsyncHandlerThriftBinaryResponse(VertxTestContext testContext) {
        client.get("/api/async-column/my_col/slice/my_slice")
                .addQueryParam("name", "my_name")
                .addQueryParam("sizeMillis", "5")
                .putHeader(RouteHandlerWrapper.RESPONSE_CONTENT_TYPE_HEADER,
                        RouteHandlerWrapper.TBINARY_B64_TYPE_VALUE)
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    if(response.statusCode() != 200) {
                        System.out.println(response.bodyAsString());
                    }
                    assertEquals(200, response.statusCode());
                    assertEquals(RouteHandlerWrapper.TBINARY_B64_TYPE_VALUE, 
                            response.getHeader("content-type"));

                    String payload = response.bodyAsString();
                    TileKey tileKey = (TileKey) RouteHandlerWrapper.deserializeTBinaryBase64(payload, TileKey.class);

                    assertEquals("my_col", tileKey.getColumn());
                    assertEquals("my_slice", tileKey.getSlice());
                    assertEquals("my_name", tileKey.getName());
                    assertEquals(5, tileKey.getSizeMillis());
                    testContext.completeNow();
                })));
    }
}
