package(default_visibility = ["//visibility:public"])

# To simulate the flink runtime environment with dependencies we want to exclude from our flink deploy jar
# We would need to specify this as 'deploy_env' for flink build target.
java_binary(
    name = "flink",
    main_class = "None",  #hack
    runtime_deps = [
        # Exclude Guava from the assembled JAR
        # Else we hit an error - java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(...)
        maven_artifact("com.google.guava:guava"),
        # Exclude protobuf from the assembled JAR
        # Else we hit an error - 'com/google/protobuf/MapField' is not assignable to 'com/google/protobuf/MapFieldReflectionAccessor'
        maven_artifact("com.google.protobuf:protobuf-java"),
        # Remove commons text as without this exclusion, Flink's JM isn't able to load the execution graph as our repo version (1.11.0) is
        # higher than Flink's version (1.10.0) and this results in the Flink UI not loading
        maven_artifact("org.apache.commons:commons-text"),
    ],
)
