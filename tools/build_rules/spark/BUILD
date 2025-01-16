package(default_visibility = ["//visibility:public"])

load("//tools/build_rules/dependencies:defs.bzl", "get_jars_for_repository")

SPARK_JARS = [
    scala_jar(
        name = "spark-core",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-catalyst",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-sql",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-hive",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-sketch",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-streaming",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-tags",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-unsafe",
        org = "org.apache.spark",
    ),
    jar(
        name = "hive-metastore",
        org = "org.apache.hive",
    ),
    jar(
        name = "hive-exec",
        org = "org.apache.hive",
    ),
    jar(
        name = "hadoop-common",
        org = "org.apache.hadoop",
    ),
    jar(
        name = "kryo_shaded",
        org = "com.esotericsoftware",
    ),
]

SPARK_3_5_JARS = SPARK_JARS + [
    scala_jar(
        name = "spark-common-utils",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-sql-api",
        org = "org.apache.spark",
    ),
]

java_library(
    name = "spark-exec",
    visibility = ["//visibility:public"],
    exports = get_jars_for_repository("spark", SPARK_3_5_JARS),
)

java_binary(
    name = "spark",
    main_class = "None",  #hack
    runtime_deps = [
        ":spark-exec",
    ],
)