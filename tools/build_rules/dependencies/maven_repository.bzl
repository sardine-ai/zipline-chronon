load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repository", "versioned_artifacts")

maven_repository = repository(
    name = "maven",
    pinned = False,
    artifacts = [
        "org.scala-lang.modules:scala-collection-compat_2.12:2.6.0",
        "org.scala-lang.modules:scala-parser-combinators_2.12:2.3.0",
        "org.scala-lang.modules:scala-java8-compat_2.12:1.0.0",
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
        "com.fasterxml.jackson.core:jackson-core:2.12.5",
        "com.fasterxml.jackson.core:jackson-databind:2.12.5",
        "com.google.code.gson:gson:2.8.6",
        "javax.annotation:javax.annotation-api:1.3.2",
    ],
    overrides = {
    },
)