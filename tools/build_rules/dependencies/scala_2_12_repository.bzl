load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repository", "versioned_artifacts")
load("@io_bazel_rules_scala_config//:config.bzl", "SCALA_MAJOR_VERSION", "SCALA_VERSION")

SCALA_2_12_REPOSITORY_NAME = "scala_2_12"

scala_2_12_repository = repository(
    name = SCALA_2_12_REPOSITORY_NAME,
    pinned = False,
    maven_install_json = None,
    artifacts = [
        "org.scala-lang:scala-library:" + SCALA_VERSION,
        "org.scala-lang:scala-reflect:" + SCALA_VERSION,
        "org.scala-lang.modules:scala-collection-compat_2.12:2.6.0",
        "org.scala-lang.modules:scala-parser-combinators_2.12:2.3.0",
        "org.scala-lang.modules:scala-java8-compat_2.12:1.0.2",

        # Unit testing
        "org.scalatestplus:mockito-3-4_2.12:3.2.10.0",
        "org.mockito:mockito-scala_2.12:1.17.0",
        "org.scalatest:scalatest_2.12:3.2.15",
        "org.scalatest:scalatest-shouldmatchers_2.12:3.2.15",
        "org.scalatest:scalatest-matchers-core_2.12:3.2.15",
        "org.scalactic:scalactic_2.12:3.2.15",
        "org.mockito:mockito-core:5.12.0",

        # Add other dependencies
        "org.apache.logging.log4j:log4j-api-scala_2.12:13.1.0",
        "com.fasterxml.jackson.module:jackson-module-scala_2.12:2.15.2",
        "org.rogach:scallop_2.12:5.1.0",
        "com.softwaremill.sttp.client3:core_2.12:3.9.7",
        "org.json4s:json4s-jackson_2.12:3.7.0-M11",
        "org.json4s:json4s-core_2.12:3.7.0-M11",
        "org.json4s:json4s-ast_2.12:3.7.0-M11",
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0",

        # Google Cloud
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.1",

        # Circe
        "io.circe:circe-core_2.12:0.14.9",
        "io.circe:circe-generic_2.12:0.14.9",
        "io.circe:circe-parser_2.12:0.14.9",
        "com.chuusai:shapeless_2.12:2.3.12",
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
