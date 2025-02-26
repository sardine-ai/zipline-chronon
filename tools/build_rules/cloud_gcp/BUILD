package(default_visibility = ["//visibility:public"])

java_binary(
    name = "cloud_gcp",
    main_class = "None",  #hack
    runtime_deps = [
        # Exclude snakeyaml from the assembled JAR
        maven_artifact("org.yaml:snakeyaml"),
    ],
)
