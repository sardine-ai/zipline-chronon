package(default_visibility = ["//visibility:public"])

java_binary(
    name = "cloud_gcp_embedded",
    main_class = "None",  #hack
    runtime_deps = [
        # Exclude snakeyaml from the assembled JAR
        maven_artifact("org.yaml:snakeyaml"),
        # pull out some slf4j-impl dependencies - these can be included at the application deploy target level if needed
        maven_artifact("org.apache.logging.log4j:log4j-slf4j2-impl"),
    ],
)
