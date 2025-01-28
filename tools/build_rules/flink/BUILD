package(default_visibility = ["//visibility:public"])

# To simulate the flink runtime environment with dependencies we want to exclude from our flink deploy jar
# We would need to specify this as 'deploy_env' for flink build target.
java_binary(
    name = "flink",
    main_class = "None",  #hack
    runtime_deps = [
        # mark the flink-streaming scala as provided as otherwise we end up with some extra Flink classes in our jar
        # and errors at runtime like: java.io.InvalidClassException: org.apache.flink.streaming.api.scala.DataStream$$anon$1; local class incompatible
        scala_artifact("org.apache.flink:flink-streaming-scala"),
        # Exclude Guava from the assembled JAR
        # Else we hit an error - java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(...)
        maven_artifact("com.google.guava:guava"),
        # Exclude protobuf from the assembled JAR
        # Else we hit an error - 'com/google/protobuf/MapField' is not assignable to 'com/google/protobuf/MapFieldReflectionAccessor'
        maven_artifact("com.google.protobuf:protobuf-java"),
    ],
)
