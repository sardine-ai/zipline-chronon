load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repository", "versioned_artifacts")

MAVEN_REPOSITORY_NAME = "maven"

maven_repository = repository(
    name = MAVEN_REPOSITORY_NAME,
    pinned = False,
    maven_install_json = "//:maven_install.json",
    artifacts = [
        # Unit testing
        "junit:junit:4.13.2",
        "org.junit.jupiter:junit-jupiter-api:5.10.5",
        "org.junit.platform:junit-platform-launcher:1.10.5",
        "org.junit.platform:junit-platform-reporting:1.10.5",
        "com.novocode:junit-interface:0.11",
        "org.mockito:mockito-core:5.12.0",
        "org.objenesis:objenesis:3.4",

        "org.eclipse.jetty:jetty-util:9.4.57.v20241219", # latest version that is still built on jdk 11 and not 17.

        # Add other dependencies
        "org.slf4j:slf4j-api:2.0.12",
        "org.apache.logging.log4j:log4j-slf4j-impl:2.20.0",
        "org.apache.logging.log4j:log4j-core:2.20.0",
        "org.apache.datasketches:datasketches-memory:3.0.2",
        "org.apache.datasketches:datasketches-java:6.1.1",
        "com.fasterxml.jackson.core:jackson-core:2.15.2",
        "com.fasterxml.jackson.core:jackson-databind:2.15.2",
        "com.fasterxml.jackson.module:jackson-module-afterburner:2.15.2",
        "com.google.code.gson:gson:2.10.1",
        "javax.annotation:javax.annotation-api:1.3.2",
        "com.datadoghq:java-dogstatsd-client:4.4.1",
        "net.jodah:typetools:0.6.3",
        "com.github.ben-manes.caffeine:caffeine:3.1.8",
        "jakarta.servlet:jakarta.servlet-api:4.0.3",
        "com.google.guava:guava:33.3.1-jre",
        "org.yaml:snakeyaml:2.3",
        "commons-io:commons-io:2.9.0",
        "commons-lang:commons-lang:2.6",
        "io.netty:netty-all:4.1.111.Final",
        "io.grpc:grpc-netty-shaded:1.62.2",
        "ch.qos.reload4j:reload4j:1.2.25",
        "ch.qos.logback:logback-classic:1.5.6",
        "com.typesafe:config:1.4.3",
        "io.micrometer:micrometer-registry-statsd:1.13.6",
        "net.sf.py4j:py4j:0.10.9.9",
        "org.apache.commons:commons-lang3:3.12.0",
        "org.apache.commons:commons-math3:3.6.1",
        "org.apache.hudi:hudi-aws-bundle:0.15.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",

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
        "org.apache.hadoop:hadoop-client-api:3.4.1",
        "org.apache.hadoop:hadoop-common:3.4.1",
        "org.apache.hadoop:hadoop-yarn-api:3.4.1",
        "org.apache.hadoop:hadoop-yarn-common:3.4.1",


        # AWS
        "software.amazon.awssdk:dynamodb:2.30.13",
        "software.amazon.awssdk:regions:2.30.13",
        "software.amazon.awssdk:aws-core:2.30.13",
        "software.amazon.awssdk:sdk-core:2.30.13",
        "software.amazon.awssdk:utils:2.30.13",
        "software.amazon.awssdk:auth:2.30.13",
        "software.amazon.awssdk:url-connection-client:2.30.13",
        "software.amazon.awssdk:identity-spi:2.30.13",
        "com.amazonaws:DynamoDBLocal:1.25.1",

        # Google Cloud
        "com.google.cloud:google-cloud-bigquery:2.42.0",
        "com.google.cloud:google-cloud-bigtable:2.41.0",
        "com.google.cloud:google-cloud-pubsub:1.131.0",
        "com.google.cloud:google-cloud-dataproc:4.52.0",
        # Have to specify in group:artifact:packaging:version format if version doesn't start with a digit
        # Code reference: https://github.com/bazel-contrib/rules_jvm_external/blob/master/private/lib/coordinates.bzl#L44
        "com.google.cloud.bigdataoss:gcs-connector:jar:hadoop3-2.2.26",
        "com.google.cloud.bigdataoss:gcsio:2.2.26",
        "com.google.cloud.bigdataoss:util-hadoop:jar:hadoop3-2.2.26",
        "com.google.cloud.bigdataoss:util:2.2.26",
        "com.google.cloud.spark:spark-3.5-bigquery:0.42.0",
        "com.google.cloud:google-cloud-bigtable-emulator:0.178.0",
        "com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.3",

        # Flink
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
    ],
    excluded_artifacts = [
        "org.apache.commons:commons-text",
        "org.apache.beam:beam-sdks-java-io-hadoop-common",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        # Exclude Hadoop from the assembled JAR
        # Else we hit an error - IllegalAccessError: class org.apache.hadoop.hdfs.web.HftpFileSystem cannot access its
        # superinterface org.apache.hadoop.hdfs.web.TokenAspect$TokenManagementDelegator
        # Note: Only excluding them from a specific module is getting tricky
        # so we ended up removing these from our entire repo as they are required across our project
        "org.apache.hadoop:hadoop-annotations",
        "org.apache.hadoop:hadoop-auth",
        "org.apache.hadoop:hadoop-hdfs-client",
        "org.apache.hadoop:hadoop-hdfs",
        "org.apache.hadoop:hadoop-mapreduce-client-core",
        "org.apache.hadoop:hadoop-yarn-client",
        "org.apache.parquet:parquet-avro",
        "org.apache.zookeeper:zookeeper",
        "org.scala-lang:scala-library",
        "org.scala-lang:scala-reflect",
    ],
    overrides = {},
)
