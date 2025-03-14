package(default_visibility = ["//visibility:public"])

java_binary(
    name = "cloud_gcp_embedded",
    main_class = "None",  #hack
    runtime_deps = [
        # Exclude snakeyaml from the assembled JAR
        maven_artifact("org.yaml:snakeyaml"),
        # pull out some slf4j-impl dependencies - these can be included at the application deploy target level if needed
        maven_artifact("org.apache.logging.log4j:log4j-slf4j2-impl"),
        # Exclude grpc deps as clients might want to use their own version
        maven_artifact("io.grpc:grpc-core:1.69.0"),
        maven_artifact("io.grpc:grpc-inprocess:1.69.0"),
        maven_artifact("io.grpc:grpc-netty-shaded"),
    ],
)
