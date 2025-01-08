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

        # Other dependencies
        "org.apache.curator:apache-curator:2.12.0",
        "com.esotericsoftware:kryo:5.1.1",
        "com.yahoo.datasketches:sketches-core:0.13.4",
        "com.yahoo.datasketches:memory:0.12.2",
        "com.yahoo.datasketches:sketches-hive:0.13.0",
        "org.apache.datasketches:datasketches-java:2.0.0",
        "org.apache.datasketches:datasketches-memory:1.3.0",

        # Kafka dependencies - only Scala 2.12
        "org.apache.kafka:kafka_2.12:2.6.3",

        # Avro dependencies
        "org.apache.avro:avro:1.8.2",
        "org.apache.avro:avro-mapred:1.8.2",
        "org.apache.hive:hive-metastore:2.3.9",
        "org.apache.hive:hive-exec:3.1.2",

        # Monitoring
        "io.prometheus.jmx:jmx_prometheus_javaagent:0.20.0",
    ],
    excluded_artifacts = [
        "org.pentaho:pentaho-aggdesigner-algorithm",
    ],
)