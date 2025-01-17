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
        "org.slf4j:slf4j-api:2.0.12",
        "org.apache.logging.log4j:log4j-slf4j-impl:2.20.0",
        "org.apache.logging.log4j:log4j-core:2.20.0",
        "org.apache.logging.log4j:log4j-api-scala_2.12:13.1.0",
        "org.apache.datasketches:datasketches-memory:3.0.2",
        "org.apache.datasketches:datasketches-java:6.1.1",
        "com.fasterxml.jackson.core:jackson-core:2.15.2",
        "com.fasterxml.jackson.core:jackson-databind:2.15.2",
        "com.fasterxml.jackson.module:jackson-module-scala_2.12:2.15.2",
        "com.google.code.gson:gson:2.10.1",
        "javax.annotation:javax.annotation-api:1.3.2",
        "com.datadoghq:java-dogstatsd-client:4.4.1",
        "org.rogach:scallop_2.12:5.1.0",
        "net.jodah:typetools:0.6.3",
        "com.github.ben-manes.caffeine:caffeine:3.1.8",
        "com.softwaremill.sttp.client3:core_2.12:3.9.7",
        "org.json4s:json4s-jackson_2.12:3.7.0-M11",
        "org.json4s:json4s-core_2.12:3.7.0-M11",
        "org.json4s:json4s-ast_2.12:3.7.0-M11",
        "jakarta.servlet:jakarta.servlet-api:4.0.3",
        "com.google.guava:guava:33.3.1-jre",
        "org.yaml:snakeyaml:2.3",
        "commons-io:commons-io:2.9.0",
        "commons-lang:commons-lang:2.6",
        "io.netty:netty-all:4.1.99.Final",
        "io.delta:delta-spark_2.12:3.2.0",

        # Kafka
        "org.apache.kafka:kafka-clients:3.9.0",
    ],
    overrides = {
    },
)