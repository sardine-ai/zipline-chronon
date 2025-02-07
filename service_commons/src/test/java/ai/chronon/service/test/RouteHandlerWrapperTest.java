package ai.chronon.service.test;

import ai.chronon.observability.TileKey;
import ai.chronon.api.TimeUnit;
import ai.chronon.api.Window;
import ai.chronon.service.RouteHandlerWrapper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;
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
        int testPort = 8888;
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


        // routes
        router.get("/api/column/:column/slice/:slice")
                .handler(RouteHandlerWrapper.createHandler(thriftTransformer, TileKey.class));

        router.get("/api/users/:userId/test")
                .handler(RouteHandlerWrapper.createHandler(transformer, TestInput.class));

        router.get("/api/window/units/:timeUnit/")
                .handler(RouteHandlerWrapper.createHandler(windowTransformer, Window.class));

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
        client.get("/api/users/123/test")
                .addQueryParam("status", "PENDING")
                .addQueryParam("role", "ADMIN")
                .addQueryParam("limit", "10")
                .addQueryParam("amount", "99.99")
                .addQueryParam("active", "true")
                .addQueryParam("items", "a,b,c")
                .addQueryParam("props", "k1:v1,k2:v2,k3:v3")
                .send()
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
        client.get("/api/users/123/test")
                .addQueryParam("status", "INVALID_STATUS")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(400, response.statusCode());
                    JsonObject error = response.bodyAsJsonObject();
                    assertTrue(error.getString("error").contains("Invalid enum value"));

                    testContext.completeNow();
                })));
    }

    @Test
    public void testCaseInsensitiveEnum(VertxTestContext testContext) {
        client.get("/api/users/123/test")
                .addQueryParam("status", "pending")  // lowercase
                .addQueryParam("role", "admin")      // lowercase
                .addQueryParam("accuracy", "temporal")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(200, response.statusCode());

                    JsonObject result = response.bodyAsJsonObject();
                    String summary = result.getString("summary");
                    assertTrue(summary.contains("Status: PENDING"));
                    assertTrue(summary.contains("Role: ADMIN"));
                    assertTrue(summary.contains("Accuracy: TEMPORAL"));
                    testContext.completeNow();
                })));
    }

    @Test
    void testSuccessfulParameterMapping(VertxTestContext testContext) {
        client.get("/api/users/123/test")
                .addQueryParam("status", "pending")
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
        client.get("/api/users/123/test")
                .addQueryParam("limit", "not-a-number")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    assertEquals(400, response.statusCode());

                    JsonObject error = response.bodyAsJsonObject();
                    assertTrue(error.getString("error").contains("not-a-number"));

                    testContext.completeNow();
                })));
    }

    @Test
    void testMissingOptionalParameters(VertxTestContext testContext) {
        client.get("/api/users/123/test")
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
        client.get("/api/users/123/test")
                .addQueryParam("status", "processing")
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
        client.get("/api/window/units/hours/")
                .addQueryParam("length", "100")
                .send()
                .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                    if(response.statusCode() != 200) {
                        System.out.println(response.bodyAsString());
                    }
                    assertEquals(200, response.statusCode());

                    JsonObject result = response.bodyAsJsonObject();

                    assertEquals(result.getString("timeUnit"), "HOURS");
                    assertEquals(result.getString("length"), "100");
                    testContext.completeNow();
                })));
    }

    @Test
    void testAllParameterTypesThriftWithEnumSerialized(VertxTestContext testContext) {
        client.get("/api/window/units/hours/")
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

    // Test POJOs
    static class SimplePojo {
        private String stringField;
        private int intField;
        private boolean boolField;

        public String getStringField() { return stringField; }
        public void setStringField(String value) { stringField = value; }

        public int getIntField() { return intField; }
        public void setIntField(int value) { intField = value; }

        public boolean isBoolField() { return boolField; }
        public void setBoolField(boolean value) { boolField = value; }
    }

    static class ComplexPojo {
        private List<String> listField;
        private Map<String, Integer> mapField;
        private TestEnum enumField;

        public List<String> getListField() { return listField; }
        public void setListField(List<String> value) { listField = value; }

        public Map<String, Integer> getMapField() { return mapField; }
        public void setMapField(Map<String, Integer> value) { mapField = value; }

        public TestEnum getEnumField() { return enumField; }
        public void setEnumField(TestEnum value) { enumField = value; }
    }

    enum TestEnum {
        FIRST, SECOND;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    @Test
    void convertSimplePojoToMap() {
        SimplePojo pojo = new SimplePojo();
        pojo.setStringField("test");
        pojo.setIntField(42);
        pojo.setBoolField(true);

        Map<String, String> result = RouteHandlerWrapper.convertPojoToMap(pojo);

        assertEquals("test", result.get("stringField"));
        assertEquals("42", result.get("intField"));
        assertEquals("true", result.get("boolField"));
    }

    @Test
    void handleNullValues() {
        SimplePojo pojo = new SimplePojo();
        pojo.setIntField(42); // only set one field

        Map<String, String> result = RouteHandlerWrapper.convertPojoToMap(pojo);

        assertFalse(result.containsKey("stringField"));
        assertEquals("42", result.get("intField"));
        assertTrue(result.containsKey("boolField")); // boolean defaults to false
    }

    @Test
    void convertListFields() {
        ComplexPojo pojo = new ComplexPojo();
        pojo.setListField(Arrays.asList("one", "two", "three"));

        Map<String, String> result = RouteHandlerWrapper.convertPojoToMap(pojo);

        assertEquals("one,two,three", result.get("listField"));
    }

    @Test
    void convertMapFields() {
        ComplexPojo pojo = new ComplexPojo();
        Map<String, Integer> map = new HashMap<>();
        map.put("key1", 1);
        map.put("key2", 2);
        pojo.setMapField(map);

        Map<String, String> result = RouteHandlerWrapper.convertPojoToMap(pojo);
        String mapFieldValue = result.get("mapField");

        // Since map ordering isn't guaranteed, check both possible orderings
        assertTrue(
                "key1:1,key2:2".equals(mapFieldValue) ||
                        "key2:2,key1:1".equals(mapFieldValue),
                "Unexpected map field value: " + mapFieldValue
        );
    }

    @Test
    void convertEnumFields() {
        ComplexPojo pojo = new ComplexPojo();
        pojo.setEnumField(TestEnum.FIRST);

        Map<String, String> result = RouteHandlerWrapper.convertPojoToMap(pojo);

        assertEquals("first", result.get("enumField")); // lowercase due to toString override
    }

    @Test
    void handleRoundTripConversion() throws Exception {
        // First create a map and convert to POJO
        Map<String, String> initialMap = new HashMap<>();
        initialMap.put("stringField", "test");
        initialMap.put("intField", "42");
        initialMap.put("boolField", "true");

        SimplePojo pojo = RouteHandlerWrapper.createInputFromParams(initialMap, SimplePojo.class);

        // Then convert back to map
        Map<String, String> resultMap = RouteHandlerWrapper.convertPojoToMap(pojo);

        // Verify the maps are equivalent
        assertEquals(initialMap.size(), resultMap.size());
        initialMap.forEach((key, value) ->
                assertEquals(value, resultMap.get(key), "Mismatch for key: " + key));
    }

    @Test
    void throwForUnsupportedTypes() {
        class UnsupportedPojo {
            public Thread getThread() { return Thread.currentThread(); }
        }

        UnsupportedPojo pojo = new UnsupportedPojo();

        assertThrows(IllegalArgumentException.class, () ->
                RouteHandlerWrapper.convertPojoToMap(pojo));
    }

    @Test
    void handleEmptyCollections() {
        ComplexPojo pojo = new ComplexPojo();
        pojo.setListField(new ArrayList<>());
        pojo.setMapField(new HashMap<>());

        Map<String, String> result = RouteHandlerWrapper.convertPojoToMap(pojo);

        assertEquals("", result.get("listField"));
        assertEquals("", result.get("mapField"));
    }
}
