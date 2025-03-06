load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repository")

SPARK_REPOSITORY_NAME = "spark"

spark_repository = repository(
    name = SPARK_REPOSITORY_NAME,
    provided = True,
    maven_install_json = "//:spark_install.json",
    artifacts = [
        # Spark artifacts - for scala 2.12
        "org.apache.spark:spark-sql_2.12:3.5.1",
        "org.apache.spark:spark-hive_2.12:3.5.1",
        "org.apache.spark:spark-streaming_2.12:3.5.1",
        "org.apache.spark:spark-avro_2.12:3.5.1",

        # Spark artifacts - for scala 2.13
        "org.apache.spark:spark-sql_2.13:3.5.1",
        "org.apache.spark:spark-hive_2.13:3.5.1",
        "org.apache.spark:spark-streaming_2.13:3.5.1",
        "org.apache.spark:spark-avro_2.13:3.5.1",

        # Hive dependencies
        "org.apache.hive:hive-metastore:2.3.9",
        "org.apache.curator:apache-curator:5.5.0",
    ],
    excluded_artifacts = [
        # Exclude commons-cli as it's picking up old 1.2 version which conflicts with our
        # Flink runtime as it needs 1.5.0 otherwise we run into this error
        # java.lang.NoSuchMethodError: 'org.apache.commons.cli.Option$Builder org.apache.commons.cli.Option.builder(java.lang.String)'
        "commons-cli:commons-cli",
        # Spark is picking up old 3.3.0 version which conflicts with our
        # Aggregator module as it needs latest 6.6.1 version.
        "org.apache.datasketches:datasketches-memory",
        "org.apache.datasketches:datasketches-java",
        "com.google.protobuf:protobuf-java",
        "com.google.protobuf:protobuf-util",
        "org.apache.hadoop:hadoop-mapreduce-client-core",
        "org.apache.hadoop:hadoop-auth",
        "org.apache.hadoop:hadoop-client-api",
        "org.apache.hadoop:hadoop-common",
        "org.apache.hadoop:hadoop-yarn-api",
        "org.apache.twill:twill-zookeeper",
        "org.apache.zookeeper:zookeeper",
        "com.google.guava:guava",
        "org.apache.orc:orc-core",
        "org.apache.orc:orc-mapreduce",
        # Exclude rocksdb from the assembled JARs that pull this in (e.g. flink, cloud_gcp) as we want to exclude
        # the rockdb library and rely on those part of the dist / env
        # Else we hit an error - NoSuchMethodError: 'void org.rocksdb.WriteBatch.remove
        "org.rocksdb:rocksdbjni",
        "org.scala-lang:scala-library",
        "org.scala-lang:scala-reflect",
    ],
)
