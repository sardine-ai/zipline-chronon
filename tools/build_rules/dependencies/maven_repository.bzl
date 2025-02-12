load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repository", "versioned_artifacts")

MAVEN_REPOSITORY_NAME = "maven"

maven_repository = repository(
    name = MAVEN_REPOSITORY_NAME,
    pinned = False,
    artifacts = [
        "org.scala-lang.modules:scala-collection-compat_2.12:2.6.0",
        "org.scala-lang.modules:scala-collection-compat_2.13:2.6.0",
        "org.scala-lang.modules:scala-parser-combinators_2.12:2.3.0",
        "org.scala-lang.modules:scala-parser-combinators_2.13:2.3.0",
        "org.scala-lang.modules:scala-java8-compat_2.12:1.0.2",
        "org.scala-lang.modules:scala-java8-compat_2.13:1.0.2",
        "org.apache.commons:commons-lang3:3.12.0",
        "org.apache.commons:commons-math3:3.6.1",

        # JUnit
        "junit:junit:4.13.2",
        "org.junit.jupiter:junit-jupiter-api:5.10.5",
        "org.junit.platform:junit-platform-launcher:1.10.5",
        "org.junit.platform:junit-platform-reporting:1.10.5",
        "com.novocode:junit-interface:0.11",
        "org.scalatestplus:mockito-3-4_2.12:3.2.10.0",
        "org.scalatestplus:mockito-3-4_2.13:3.2.10.0",
        "org.mockito:mockito-core:5.12.0",
        "org.mockito:mockito-scala_2.12:1.17.0",
        "org.mockito:mockito-scala_2.13:1.17.0",
        "org.scalatest:scalatest_2.12:3.2.15",
        "org.scalatest:scalatest_2.13:3.2.15",
        "org.scalatest:scalatest-shouldmatchers_2.12:3.2.15",
        "org.scalatest:scalatest-shouldmatchers_2.13:3.2.15",
        "org.scalatest:scalatest-matchers-core_2.12:3.2.15",
        "org.scalatest:scalatest-matchers-core_2.13:3.2.15",
        "org.scalactic:scalactic_2.12:3.2.15",
        "org.scalactic:scalactic_2.13:3.2.15",

        # Add other dependencies
        "org.slf4j:slf4j-api:2.0.12",
        "org.apache.logging.log4j:log4j-slf4j-impl:2.20.0",
        "org.apache.logging.log4j:log4j-core:2.20.0",
        "org.apache.logging.log4j:log4j-api-scala_2.12:13.1.0",
        "org.apache.logging.log4j:log4j-api-scala_2.13:13.1.0",
        "org.apache.datasketches:datasketches-memory:3.0.2",
        "org.apache.datasketches:datasketches-java:6.1.1",
        "com.fasterxml.jackson.core:jackson-core:2.15.2",
        "com.fasterxml.jackson.core:jackson-databind:2.15.2",
        "com.fasterxml.jackson.module:jackson-module-scala_2.12:2.15.2",
        "com.fasterxml.jackson.module:jackson-module-scala_2.13:2.15.2",
        "com.google.code.gson:gson:2.10.1",
        "javax.annotation:javax.annotation-api:1.3.2",
        "com.datadoghq:java-dogstatsd-client:4.4.1",
        "org.rogach:scallop_2.12:5.1.0",
        "org.rogach:scallop_2.13:5.1.0",
        "net.jodah:typetools:0.6.3",
        "com.github.ben-manes.caffeine:caffeine:3.1.8",
        "com.softwaremill.sttp.client3:core_2.12:3.9.7",
        "com.softwaremill.sttp.client3:core_2.13:3.9.7",
        "org.json4s:json4s-jackson_2.12:3.7.0-M11",
        "org.json4s:json4s-jackson_2.13:3.7.0-M11",
        "org.json4s:json4s-core_2.12:3.7.0-M11",
        "org.json4s:json4s-core_2.13:3.7.0-M11",
        "org.json4s:json4s-ast_2.12:3.7.0-M11",
        "org.json4s:json4s-ast_2.13:3.7.0-M11",
        "jakarta.servlet:jakarta.servlet-api:4.0.3",
        "com.google.guava:guava:33.3.1-jre",
        "org.yaml:snakeyaml:2.3",
        "commons-io:commons-io:2.9.0",
        "commons-lang:commons-lang:2.6",
        "io.netty:netty-all:4.1.111.Final",
        "io.delta:delta-spark_2.12:3.2.0",
        "io.delta:delta-spark_2.13:3.2.0",
        "io.grpc:grpc-netty-shaded:1.62.2",
        "ch.qos.reload4j:reload4j:1.2.25",
        "ch.qos.logback:logback-classic:1.5.6",
        "com.typesafe:config:1.4.3",
        "io.micrometer:micrometer-registry-statsd:1.13.6",
        "net.sf.py4j:py4j:0.10.9.9",

        # grpc
        "io.grpc:grpc-core:1.62.2",  # required by bigtable
        "io.grpc:grpc-api:1.62.2",

        # Kafka
        "org.apache.kafka:kafka-clients:3.8.1",
        "io.confluent:kafka-schema-registry-client:7.8.0",
        "io.confluent:kafka-protobuf-provider:7.8.0",
        "com.google.protobuf:protobuf-java-util:3.25.1",
        "com.google.protobuf:protobuf-java:3.25.1",

        # Avro
        "org.apache.avro:avro:1.11.3",

        # Hive
        "org.apache.hive:hive-metastore:2.3.9",
        # !!! this is a dangerous dependency - only used in //online:test-lib - please don't use it anywhere else
        "org.apache.hive:hive-exec:2.3.9",

        # Hadoop
        "org.apache.hadoop:hadoop-client-api:3.3.4",

        # Google Cloud
        "com.google.cloud:google-cloud-bigquery:2.42.0",
        "com.google.cloud:google-cloud-bigtable:2.41.0",
        "com.google.cloud:google-cloud-pubsub:1.131.0",
        "com.google.cloud:google-cloud-dataproc:4.52.0",
        # Have to specify in group:artifact:packaging:version format if version doesn't start with a digit
        # Code reference: https://github.com/bazel-contrib/rules_jvm_external/blob/master/private/lib/coordinates.bzl#L44
        "com.google.cloud.bigdataoss:gcs-connector:jar:hadoop3-2.2.26",
        "com.google.cloud.bigdataoss:gcsio:3.0.3",
        "com.google.cloud.bigdataoss:util-hadoop:3.0.0",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.1",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.41.1",
        "com.google.cloud:google-cloud-bigtable-emulator:0.178.0",
        "com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.3",

        # Flink
#         "org.apache.flink:flink-streaming-scala_2.12:1.17.0",
#         "org.apache.flink:flink-streaming-scala_2.13:1.17.0",
        "org.apache.flink:flink-metrics-dropwizard:1.17.0",
        "org.apache.flink:flink-clients:1.17.0",
        "org.apache.flink:flink-yarn:1.17.0",
        "org.apache.flink:flink-runtime:1.17.0",
        "org.apache.flink:flink-connector-kafka:1.17.0",
        "org.apache.flink:flink-connector-files:1.17.0",
        "org.apache.flink:flink-avro:1.17.0",
        "org.apache.flink:flink-runtime:1.17.0:tests",
        "org.apache.flink:flink-test-utils:1.17.0",

        # Vertx
        "io.vertx:vertx-core:4.5.10",
        "io.vertx:vertx-web:4.5.10",
        "io.vertx:vertx-web-client:4.5.10",
        "io.vertx:vertx-config:4.5.10",
        "io.vertx:vertx-micrometer-metrics:4.5.10",
        "io.vertx:vertx-junit5:4.5.10",
        "io.vertx:vertx-unit:4.5.10",
        "io.vertx:vertx-unit:4.5.10",

        # Circe
        "io.circe:circe-core_2.12:0.14.9",
        "io.circe:circe-core_2.13:0.14.9",
        "io.circe:circe-generic_2.12:0.14.9",
        "io.circe:circe-generic_2.13:0.14.9",
        "io.circe:circe-parser_2.12:0.14.9",
        "io.circe:circe-parser_2.13:0.14.9",
    ],
    excluded_artifacts = [
        "org.pentaho:pentaho-aggdesigner-algorithm",
        # Exclude Hadoop from the assembled JAR
        # Else we hit an error - IllegalAccessError: class org.apache.hadoop.hdfs.web.HftpFileSystem cannot access its
        # superinterface org.apache.hadoop.hdfs.web.TokenAspect$TokenManagementDelegator
        # Note: Only excluding them from a specific module is getting tricky
        # so we ended up removing these from our entire repo as they are required across our project
        "org.apache.hadoop:hadoop-annotations",
        "org.apache.hadoop:hadoop-auth",
        "org.apache.hadoop:hadoop-common",
        "org.apache.hadoop:hadoop-hdfs-client",
        "org.apache.hadoop:hadoop-hdfs",
        "org.apache.hadoop:hadoop-mapreduce-client-core",
        "org.apache.hadoop:hadoop-yarn-api",
        "org.apache.hadoop:hadoop-yarn-client",
        "org.apache.hadoop:hadoop-yarn-common",
    ],
    overrides = {},
)
