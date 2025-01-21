load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repository")

spark_repository = repository(
    name = "spark",
    provided = True,
    artifacts = [
        # Spark artifacts - for scala 2.12
        "org.apache.spark:spark-sql_2.12:3.5.1",
        "org.apache.spark:spark-hive_2.12:3.5.1",
        "org.apache.spark:spark-streaming_2.12:3.5.1",

        # Hive dependencies
        "org.apache.hive:hive-metastore:2.3.9",
        "org.apache.hive:hive-exec:2.3.9",

        "org.apache.curator:apache-curator:5.5.0",
    ],
    excluded_artifacts = [
        "org.pentaho:pentaho-aggdesigner-algorithm",
    ],
)