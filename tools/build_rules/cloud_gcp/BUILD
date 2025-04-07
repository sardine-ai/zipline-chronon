package(default_visibility = ["//visibility:public"])

java_binary(
    name = "cloud_gcp",
    main_class = "None",  #hack
    runtime_deps = [
        # Exclude snakeyaml from the assembled JAR
        maven_artifact("org.yaml:snakeyaml"),
        # Remove commons text as without this exclusion, Flink's JM isn't able to load the execution graph as our repo version (1.11.0) is
        # higher than Flink's version (1.10.0) and this results in the Flink UI not loading
        maven_artifact("org.apache.commons:commons-text"),
        # pull out some slf4j-impl dependencies - these can be included at the application deploy target level if needed
    ],
)
