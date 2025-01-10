load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repository", "versioned_artifacts")

maven_repository = repository(
    name = "maven",
    pinned = False,
    artifacts = [
        "org.scala-lang.modules:scala-collection-compat_2.12:2.6.0",
        "org.scala-lang.modules:scala-parser-combinators_2.12:2.3.0",
        "org.scala-lang.modules:scala-java8-compat_2.12:1.0.2",
        "org.apache.commons:commons-lang3:3.12.0",
        "org.apache.commons:commons-math3:3.6.1",

        # JUnit
        "junit:junit:4.13.2",
        "com.novocode:junit-interface:0.11",
        "org.scalatestplus:mockito-3-4_2.12:3.2.10.0",
        "org.mockito:mockito-core:4.6.1",
        "org.mockito:mockito-scala_2.12:1.17.0",
        "org.scalatest:scalatest_2.12:3.2.15",
        "org.scalatest:scalatest-shouldmatchers_2.12:3.2.15",
        "org.scalatest:scalatest-matchers-core_2.12:3.2.15",
        "org.scalactic:scalactic_2.12:3.2.15",

        # Add other dependencies
        "org.slf4j:slf4j-api:1.7.30",
        "org.slf4j:slf4j-log4j12:1.7.30",
        "org.apache.datasketches:datasketches-memory:3.0.2",
        "org.apache.datasketches:datasketches-java:6.1.1",
        "com.fasterxml.jackson.core:jackson-core:2.12.5",
        "com.fasterxml.jackson.core:jackson-databind:2.12.5",
        "com.google.code.gson:gson:2.10.1",
        "javax.annotation:javax.annotation-api:1.3.2",
        "com.datadoghq:java-dogstatsd-client:4.4.1",
        "org.rogach:scallop_2.12:5.1.0",
        "net.jodah:typetools:0.6.3",
        "com.github.ben-manes.caffeine:caffeine:3.1.8",
        "com.softwaremill.sttp.client3:core_2.12:3.9.7",
        "org.json4s:json4s-jackson_2.12:3.6.12",
        "org.json4s:json4s-core_2.12:3.6.12",
        "org.json4s:json4s-ast_2.12:3.6.12",

        # Flink
        versioned_artifacts("1.17.0", [
            "org.apache.flink:flink-clients",
            "org.apache.flink:flink-connector-files",
            "org.apache.flink:flink-connector-hive_2.12",
            "org.apache.flink:flink-csv",
            "org.apache.flink:flink-json",
            "org.apache.flink:flink-metrics-core",
            "org.apache.flink:flink-metrics-prometheus:jar",
            "org.apache.flink:flink-orc",
            "org.apache.flink:flink-parquet",
            "org.apache.flink:flink-protobuf",
            "org.apache.flink:flink-scala_2.12",
            "org.apache.flink:flink-sql-gateway-api",
            "org.apache.flink:flink-streaming-java",
            "org.apache.flink:flink-streaming-scala_2.12",
            "org.apache.flink:flink-table-api-java",
            "org.apache.flink:flink-table-planner_2.12",
            "org.apache.flink:flink-test-utils",
            "org.apache.flink:flink-streaming-java:jar:tests",
            "org.apache.flink:flink-metrics-dropwizard",
        ]),
    ],
    overrides = {
    },
)