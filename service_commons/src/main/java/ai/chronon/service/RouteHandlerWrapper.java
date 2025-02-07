package ai.chronon.service;

import ai.chronon.api.thrift.*;
import ai.chronon.api.thrift.protocol.TBinaryProtocol;
import ai.chronon.api.thrift.protocol.TSimpleJSONProtocol;
import ai.chronon.api.thrift.transport.TTransportException;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Map<Class<?>, Map<String, Method>> SETTER_CACHE = new ConcurrentHashMap<>();

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
                // Create map with path parameters
                Map<String, String> params = new HashMap<>(ctx.pathParams());

                // Add query parameters
                for (Map.Entry<String, String> entry : ctx.queryParams().entries()) {
                    params.put(entry.getKey(), entry.getValue());
                }

                I input = createInputFromParams(params, inputClass);
                O output = transformer.apply(input);

                String responseFormat = ctx.request().getHeader(RESPONSE_CONTENT_TYPE_HEADER);
                if (responseFormat == null || responseFormat.equals("application/json")) {
                    try {
                        TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
                        String jsonString = serializer.toString((TBase)output);
                        ctx.response()
                            .setStatusCode(200)
                            .putHeader("content-type", JSON_TYPE_VALUE)
                            .end(jsonString);
                    } catch (TException e) {
                        LOGGER.error("Failed to serialize response", e);
                        throw new RuntimeException(e);
                    } catch (Exception e) {
                        LOGGER.error("Unexpected error during serialization", e);
                        throw new RuntimeException(e);
                    }
                } else {
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

                    ctx.response().setStatusCode(200).putHeader("content-type", TBINARY_B64_TYPE_VALUE).end(responseBase64);
                }
            } catch (IllegalArgumentException ex) {
                LOGGER.error("Incorrect arguments passed for handler creation", ex);
                ctx.response().setStatusCode(400).putHeader("content-type", "application/json").end(new JsonObject().put("error", ex.getMessage()).encode());
            } catch (Exception ex) {
                LOGGER.error("Internal error occurred during handler creation", ex);
                ctx.response().setStatusCode(500).putHeader("content-type", "application/json").end(new JsonObject().put("error", ex.getMessage()).encode());
            }
        };
    }

    public static <I> I createInputFromParams(Map<String, String> params, Class<I> inputClass) throws Exception {
        // Create new instance using no-args constructor
        I input = inputClass.getDeclaredConstructor().newInstance();


        Map<String, Method> setters = SETTER_CACHE.computeIfAbsent(inputClass, cls ->
                Arrays.stream(cls.getMethods())
                    .filter(RouteHandlerWrapper::isSetter)
                    .collect(Collectors.toMap(RouteHandlerWrapper::getFieldNameFromSetter, method -> method))
        );

        // Find and invoke setters for matching parameters
        for (Map.Entry<String, String> param : params.entrySet()) {
            Method setter = setters.get(param.getKey());
            if (setter != null) {
                String paramValue = param.getValue();

                if (paramValue != null) {
                    Type paramType = setter.getGenericParameterTypes()[0];
                    Object convertedValue = convertValue(paramValue, paramType);
                    setter.invoke(input, convertedValue);
                }
            }
        }

        return input;
    }

    private static boolean isSetter(Method method) {
        return method.getName().startsWith("set") && !method.getName().endsWith("IsSet") && method.getParameterCount() == 1 && (method.getReturnType() == void.class || method.getReturnType() == method.getDeclaringClass());
    }

    private static String getFieldNameFromSetter(Method method) {
        String methodName = method.getName();
        String fieldName = methodName.substring(3); // Remove "set"
        return fieldName.substring(0, 1).toLowerCase() + fieldName.substring(1);
    }

    private static Object convertValue(String value, Type targetType) {  // Changed parameter to Type
        // Handle Class types
        if (targetType instanceof Class) {
            Class<?> targetClass = (Class<?>) targetType;

            if (targetClass == String.class) {
                return value;
            } else if (targetClass == Integer.class || targetClass == int.class) {
                return Integer.parseInt(value);
            } else if (targetClass == Long.class || targetClass == long.class) {
                return Long.parseLong(value);
            } else if (targetClass == Double.class || targetClass == double.class) {
                return Double.parseDouble(value);
            } else if (targetClass == Boolean.class || targetClass == boolean.class) {
                return Boolean.parseBoolean(value);
            } else if (targetClass == Float.class || targetClass == float.class) {
                return Float.parseFloat(value);
            } else if (targetClass.isEnum()) {
                try {
                    // Try custom fromString method first
                    Method fromString = targetClass.getMethod("fromString", String.class);
                    Object result = fromString.invoke(null, value);
                    if (value != null && result == null) {
                        throw new IllegalArgumentException(String.format("Invalid enum value %s for type %s", value, targetClass.getSimpleName()));
                    }
                    return result;
                } catch (NoSuchMethodException e) {
                    // Fall back to standard enum valueOf
                    return Enum.valueOf(targetClass.asSubclass(Enum.class), value.toUpperCase());
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format("Error converting %s to enum type %s : %s", value, targetClass.getSimpleName(), e.getMessage()));
                }
            }
        }

        // Handle parameterized types (List, Map)
        if (targetType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) targetType;
            Class<?> rawType = (Class<?>) parameterizedType.getRawType();

            // Handle List types
            if (List.class.isAssignableFrom(rawType)) {
                Type elementType = parameterizedType.getActualTypeArguments()[0];
                return Arrays
                        .stream(value.split(","))
                        .map(v -> convertValue(v.trim(), elementType))
                        .collect(Collectors.toList());
            }

            // Handle Map types
            if (Map.class.isAssignableFrom(rawType)) {
                Type keyType = parameterizedType.getActualTypeArguments()[0];
                Type valueType = parameterizedType.getActualTypeArguments()[1];
                return Arrays
                        .stream(value.split(","))
                        .map(entry -> entry.split(":"))
                        .filter(kv -> {
                            if (kv.length != 2) {
                                throw new IllegalArgumentException("Invalid map entry format. Expected 'key:value' but got: " + String.join(":", kv));
                            }
                            return true;
                        })
                        .collect(Collectors.toMap(
                                kv -> convertValue(kv[0].trim(), keyType),
                                kv -> convertValue(kv[1].trim(), valueType)
                        ));
            }
        }

        throw new IllegalArgumentException("Unsupported type: " + targetType.getTypeName());
    }
}
