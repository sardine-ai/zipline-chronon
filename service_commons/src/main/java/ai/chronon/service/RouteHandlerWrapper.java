package ai.chronon.service;

import ai.chronon.api.thrift.*;
import ai.chronon.api.thrift.protocol.TBinaryProtocol;
import ai.chronon.api.thrift.protocol.TSimpleJSONProtocol;
import ai.chronon.api.thrift.transport.TTransportException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Base64;
import java.util.Map;
import java.util.function.Function;

/**
 * Wrapper class for creating Route handlers that map parameters to an Input object and transform it to Output
 * The wrapped handler produces a JSON response.
 * TODO: Add support for Thrift BinaryProtocol based serialization based on a special request query param.
 */
public class RouteHandlerWrapper {

    public static String RESPONSE_CONTENT_TYPE_HEADER = "response-content-type";
    public static String TBINARY_B64_TYPE_VALUE = "application/tbinary-b64";
    public static String JSON_TYPE_VALUE = "application/json";

    private static final Logger LOGGER = LoggerFactory.getLogger(RouteHandlerWrapper.class.getName());

    private static final ThreadLocal<TSerializer> binarySerializer = ThreadLocal.withInitial(() -> {
        try {
            return new TSerializer(new TBinaryProtocol.Factory());
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    });

    private static final ThreadLocal<TDeserializer> binaryDeSerializer = ThreadLocal.withInitial(() -> {
        try {
            return new TDeserializer(new TBinaryProtocol.Factory());
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    });

    private static final ThreadLocal<Base64.Encoder> base64Encoder = ThreadLocal.withInitial(Base64::getEncoder);
    private static final ThreadLocal<Base64.Decoder> base64Decoder = ThreadLocal.withInitial(Base64::getDecoder);

    public static <T extends TBase> T deserializeTBinaryBase64(String base64Data, Class<? extends TBase> clazz) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, TException {
        byte[] binaryData = base64Decoder.get().decode(base64Data);
        T tb = (T) clazz.getDeclaredConstructor().newInstance();
        binaryDeSerializer.get().deserialize(tb, binaryData);
        return tb;
    }

    /**
     * Combines path parameters, query parameters, and JSON body into a single JSON object.
     * Returns the JSON object as a string.
     */
    public static String combinedParamJson(RoutingContext ctx) {
        JsonObject params = ctx.body().asJsonObject();
        if (params == null) {
            params = new JsonObject();
        }

        // Add path parameters
        for (Map.Entry<String, String> entry : ctx.pathParams().entrySet()) {
            params.put(entry.getKey(), entry.getValue());
        }

        // Add query parameters
        for (Map.Entry<String, String> entry : ctx.queryParams().entries()) {
            params.put(entry.getKey(), entry.getValue());
        }

        return params.encodePrettily();
    }

    /**
     * Creates a RoutingContext handler that maps parameters to an Input object and transforms it to Output
     *
     * @param transformer Function to convert from Input to Output
     * @param inputClass  Class object for the Input type
     * @param <I>         Input type with setter methods
     * @param <O>         Output type
     * @return Handler for RoutingContext that produces Output
     * TODO: To use consistent helper wrappers for the response.
     */
    public static <I, O> Handler<RoutingContext> createHandler(Function<I, O> transformer, Class<I> inputClass) {

        return ctx -> {
            try {
                String encodedParams = combinedParamJson(ctx);

                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

                I input = mapper.readValue(encodedParams, inputClass);
                O output = transformer.apply(input);

                String responseFormat = ctx.request().getHeader(RESPONSE_CONTENT_TYPE_HEADER);
                if (responseFormat == null || responseFormat.equals("application/json")) {
                    String outputJson = outputToJson(ctx, output);
                    ctx.response()
                            .setStatusCode(200)
                            .putHeader("content-type", JSON_TYPE_VALUE)
                            .end(outputJson);
                } else {
                    String responseBase64 = convertToTBinaryB64(responseFormat, output);
                    ctx.response().setStatusCode(200).putHeader("content-type", TBINARY_B64_TYPE_VALUE).end(responseBase64);
                }

            } catch (IllegalArgumentException ex) {
                LOGGER.error("Incorrect arguments passed for handler creation", ex);
                ctx.response()
                        .setStatusCode(400)
                        .putHeader("content-type", "application/json")
                        .end(toErrorPayload(ex));
            } catch (Exception ex) {
                LOGGER.error("Internal error occurred during handler creation", ex);
                ctx.response()
                        .setStatusCode(500)
                        .putHeader("content-type", "application/json")
                        .end(toErrorPayload(ex));
            }
        };
    }

    private static <O> String convertToTBinaryB64(String responseFormat, O output) throws TException {
        if (!responseFormat.equals(TBINARY_B64_TYPE_VALUE)) {
            throw new IllegalArgumentException(String.format("Unsupported response-content-type: %s. Supported values are: %s and %s", responseFormat, JSON_TYPE_VALUE, TBINARY_B64_TYPE_VALUE));
        }

        // Verify output is a Thrift object before casting
        if (!(output instanceof TBase)) {
            throw new IllegalArgumentException("Output must be a Thrift object for binary serialization");
        }
        TBase<?, TFieldIdEnum> tb = (TBase<?, TFieldIdEnum>) output;
        // Serialize output to Thrift BinaryProtocol
        byte[] serializedOutput = binarySerializer.get().serialize(tb);
        String responseBase64 = base64Encoder.get().encodeToString(serializedOutput);
        return responseBase64;
    }

    private static <O> String outputToJson(RoutingContext ctx, O output) {
        try {
            String jsonString;
            if (output instanceof TBase) {
                // For Thrift objects, use TSerializer
                TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
                jsonString = serializer.toString((TBase) output);
            } else {
                // For regular Java objects, use Vertx's JSON support
                JsonObject jsonObject = new JsonObject(Json.encode(output));
                jsonString = jsonObject.encode();
            }
            return jsonString;
        } catch (TException e) {
            LOGGER.error("Failed to serialize response", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOGGER.error("Unexpected error during serialization", e);
            throw new RuntimeException(e);
        }
    }

    public static String toErrorPayload(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return new JsonObject().put("error", sw.getBuffer().toString()).encode();
    }
}
