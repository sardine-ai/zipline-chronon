import sbt.Keys.{dependencyOverrides, libraryDependencies, *}
import sbt.{Test, *}
import sbt.Tests.{Group, SubProcess}

import scala.collection.Seq

// Notes about a few dependencies - and how we land on versions
// Our approach is to use the latest stable versions of deps as of today (July 24) and pin to them for a few years
// this should simplify our build setup, speed up CI and deployment

// latest dataproc and emr versions at the time (July 2024) of writing this comment are 2.2.x and 7.1.0 respectively
// google dataproc versions 2.2.x: https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.2
// flink is at 1.17.0, spark is at 3.5.0, scala is at 2.12.17, Java 11

// emr 7.1.0 versions:https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-710-release.html
// spark is at 3.5.0, flink is at 1.18.1, scala is at 2.12.18, Java 17

// java incompatibility is probably not an issue, hopefully we can cross build flink 1.17 & 1.18 without code changes

lazy val scala_2_12 = "2.12.18"
lazy val scala_2_13 = "2.13.14"

// spark deps: https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.12/3.5.0
// avro 1.11.2, jackson: 2.15.2
lazy val spark_3_5 = "3.5.1"
// flink deps: https://mvnrepository.com/artifact/org.apache.flink/flink-java/1.17.1
// jackson is shaded 2.13-2.16, no avro dependency
lazy val flink_1_17 = "1.17.0"
lazy val jackson_2_15 = "2.15.2"
lazy val avro_1_11 = "1.11.2"
lazy val circeVersion = "0.14.9"
lazy val deltaVersion = "3.2.0"
lazy val slf4jApiVersion = "2.0.12"
lazy val logbackClassicVersion = "1.5.6"
lazy val vertxVersion = "4.5.10"

// skip tests on assembly - uncomment if builds become slow
// ThisBuild / assembly / test := {}

ThisBuild / scalaVersion := scala_2_12

inThisBuild(
  List(
    scalaVersion := "2.12.18",
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    scalacOptions += {
      if (scalaVersion.value.startsWith("2.12"))
        "-Ywarn-unused"
      else
        "-Wunused:imports"
    }
  )
)

lazy val supportedVersions = List(scala_2_12) // List(scala211, scala212, scala213)

lazy val root = (project in file("."))
  .aggregate(api, aggregator, online, spark, flink, cloud_gcp, cloud_aws, service_commons, service, hub)
  .settings(name := "chronon")

val spark_sql = Seq(
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-core"
).map(_ % spark_3_5)
val spark_sql_provided = spark_sql.map(_ % "provided")

val delta = Seq(
  "io.delta" %% "delta-spark"
).map(_ % deltaVersion)

val spark_all = Seq(
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-hive",
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-streaming",
  "org.apache.spark" %% "spark-sql-kafka-0-10"
).map(_ % spark_3_5) :+ (
  "javax.servlet" % "javax.servlet-api" % "3.1.0",
)
val spark_all_provided = spark_all.map(_ % "provided")

val log4j2 = Seq("org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0")

val jackson = Seq(
  "com.fasterxml.jackson.core" % "jackson-core",
  "com.fasterxml.jackson.core" % "jackson-databind",
  "com.fasterxml.jackson.module" %% "jackson-module-scala"
).map(_ % jackson_2_15)

// Circe is used to ser / deser case class payloads for the Hub Play webservice
val circe = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

val flink_all = Seq(
  "org.apache.flink" %% "flink-streaming-scala",
  "org.apache.flink" % "flink-metrics-dropwizard",
  "org.apache.flink" % "flink-clients"
).map(_ % flink_1_17)

val vertx_java = Seq(
  "io.vertx" % "vertx-core",
  "io.vertx" % "vertx-web",
  "io.vertx" % "vertx-config",
  // wire up metrics using micro meter and statsd
  "io.vertx" % "vertx-micrometer-metrics"
).map(_ % vertxVersion)

val avro = Seq("org.apache.avro" % "avro" % "1.11.3")

lazy val api = project
  .settings(
    Compile / sourceGenerators += Def.task {
      val inputThrift = baseDirectory.value / "thrift" / "api.thrift"
      val outputJava = (Compile / sourceManaged).value
      Thrift.gen(inputThrift.getPath, outputJava.getPath, "java")
    }.taskValue,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= spark_sql_provided,
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % "test"
    )
  )

lazy val aggregator = project
  .dependsOn(api.%("compile->compile;test->test"))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.datasketches" % "datasketches-java" % "6.1.0",
      "com.google.code.gson" % "gson" % "2.10.1"
    ),
    libraryDependencies ++= spark_sql_provided
  )

// todo add a service module with spark as a hard dependency
lazy val online = project
  .dependsOn(aggregator.%("compile->compile;test->test"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
      "com.datadoghq" % "java-dogstatsd-client" % "4.4.1",
      "org.rogach" %% "scallop" % "5.1.0",
      "net.jodah" % "typetools" % "0.6.3",
      "com.github.ben-manes.caffeine" % "caffeine" % "3.1.8"
    ),
    libraryDependencies ++= jackson,
    // dep needed for HTTPKvStore - yank when we rip this out
    libraryDependencies += "com.softwaremill.sttp.client3" %% "core" % "3.9.7",
    libraryDependencies ++= spark_all.map(_ % "provided"),
    libraryDependencies ++= flink_all.map(_ % "provided")
  )

lazy val tmp_warehouse = "/tmp/chronon/"
def cleanSparkMeta(): Unit = {
  Folder.clean(file(".") / "spark" / "spark-warehouse",
               file(tmp_warehouse) / "spark-warehouse",
               file(".") / "spark" / "metastore_db",
               file(tmp_warehouse) / "metastore_db")
}

val sparkBaseSettings: Seq[Setting[_]] = Seq(
  assembly / test := {},
  assembly / artifact := {
    val art = (assembly / artifact).value
    art.withClassifier(Some("assembly"))
  },
  mainClass in (Compile, run) := Some("ai.chronon.spark.Driver"),
  cleanFiles ++= Seq(file(tmp_warehouse)),
  Test / testOptions += Tests.Setup(() => cleanSparkMeta()),
  // compatibility for m1 chip laptop
  libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.10.4" % Test
) ++ addArtifact(assembly / artifact, assembly)

lazy val spark = project
  .dependsOn(aggregator.%("compile->compile;test->test"), online)
  .settings(
    sparkBaseSettings,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= spark_all_provided,
    libraryDependencies ++= spark_all.map(_ % "test"),
    libraryDependencies += "jakarta.servlet" % "jakarta.servlet-api" % "4.0.3",
    libraryDependencies += "com.google.guava" % "guava" % "33.3.1-jre",
    libraryDependencies ++= log4j2,
    libraryDependencies ++= delta.map(_ % "provided")
  )

lazy val flink = project
  .dependsOn(aggregator.%("compile->compile;test->test"), online)
  .settings(
    libraryDependencies ++= spark_all,
    libraryDependencies ++= flink_all,
    libraryDependencies += "org.apache.flink" % "flink-test-utils" % flink_1_17 % Test excludeAll (
      ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-api"),
      ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-core"),
      ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-slf4j-impl")
    )
  )

// GCP requires java 11, can't cross compile higher
lazy val cloud_gcp = project
  .dependsOn(api % ("compile->compile;test->test"), online, spark % ("compile->compile;test->test"))
  .settings(
    libraryDependencies += "com.google.cloud" % "google-cloud-bigquery" % "2.42.0",
    libraryDependencies += "com.google.cloud" % "google-cloud-bigtable" % "2.41.0",
    libraryDependencies += "com.google.cloud" % "google-cloud-pubsub" % "1.131.0",
    libraryDependencies += "com.google.cloud" % "google-cloud-dataproc" % "4.51.0",
    libraryDependencies += "io.circe" %% "circe-yaml" % "1.15.0",
    libraryDependencies += "org.mockito" % "mockito-core" % "5.12.0" % Test,
    libraryDependencies += "com.google.cloud.spark" %% s"spark-bigquery-with-dependencies" % "0.41.0",
    libraryDependencies ++= circe,
    libraryDependencies ++= avro,
    libraryDependencies ++= spark_all_provided,
    dependencyOverrides ++= jackson
  )

lazy val cloud_aws = project
  .dependsOn(api.%("compile->compile;test->test"), online)
  .settings(
    libraryDependencies += "software.amazon.awssdk" % "dynamodb" % "2.25.35",
    libraryDependencies += "com.amazonaws" % "DynamoDBLocal" % "2.5.1" % "test",
    libraryDependencies += "io.circe" %% "circe-core" % circeVersion % "test",
    libraryDependencies += "io.circe" %% "circe-generic" % circeVersion % "test",
    libraryDependencies += "io.circe" %% "circe-parser" % circeVersion % "test",
    libraryDependencies += "com.google.guava" % "guava" % "33.3.1-jre",
    libraryDependencies ++= spark_all
  )

// Webpack integration for frontend
lazy val buildFrontend = taskKey[Unit]("Build frontend")

lazy val frontend = (project in file("frontend"))
  .settings(
    buildFrontend := {
      println("Installing frontend dependencies...")
      import scala.sys.process._
      val npmCiResult = Process("npm ci", file("frontend")).!

      if (npmCiResult != 0) {
        sys.error("npm ci failed!")
      }

      println("Building frontend...")
      val buildResult = Process("npm run build", file("frontend")).!

      if (buildResult == 0) {
        println("Copying frontend assets to /hub/public...")
        val buildDir = file("frontend/build")
        val publicDir = file("hub/public")

        // Clean the target directory if needed
        IO.delete(publicDir)

        // Copy the build files to the public folder
        IO.copyDirectory(buildDir, publicDir)
      } else {
        sys.error("Frontend build failed!")
      }
    }
  )

lazy val service_commons = (project in file("service_commons"))
  .dependsOn(online)
  .settings(
    libraryDependencies ++= vertx_java,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackClassicVersion,
      "org.slf4j" % "slf4j-api" % slf4jApiVersion,
      "com.typesafe" % "config" % "1.4.3",
      // force netty versions -> without this we conflict with the versions pulled in from
      // our online module's spark deps which causes the web-app to not serve up content
      "io.netty" % "netty-all" % "4.1.111.Final",
      // wire up metrics using micro meter and statsd
      "io.micrometer" % "micrometer-registry-statsd" % "1.13.6"
    )
  )

lazy val service = (project in file("service"))
  .dependsOn(online, service_commons)
  .settings(
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    assembly / artifact := {
      val art = (assembly / artifact).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(assembly / artifact, assembly),
    libraryDependencies ++= vertx_java,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackClassicVersion,
      "org.slf4j" % "slf4j-api" % slf4jApiVersion,
      "com.typesafe" % "config" % "1.4.3",
      // force netty versions -> without this we conflict with the versions pulled in from
      // our online module's spark deps which causes the web-app to not serve up content
      "io.netty" % "netty-all" % "4.1.111.Final",
      // wire up metrics using micro meter and statsd
      "io.micrometer" % "micrometer-registry-statsd" % "1.13.6",
      "junit" % "junit" % "4.13.2" % Test,
      "com.novocode" % "junit-interface" % "0.11" % Test,
      "org.mockito" % "mockito-core" % "5.12.0" % Test,
      "io.vertx" % "vertx-unit" % vertxVersion % Test
    ),
    // Assembly settings
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    // Main class configuration
    // We use a custom launcher to help us wire up our statsd metrics
    Compile / mainClass := Some("ai.chronon.service.ChrononServiceLauncher"),
    assembly / mainClass := Some("ai.chronon.service.ChrononServiceLauncher"),
    // Merge strategy for assembly
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF")           => MergeStrategy.discard
      case PathList("META-INF", xs @ _*)                 => MergeStrategy.first
      case PathList("javax", "activation", xs @ _*)      => MergeStrategy.first
      case PathList("org", "apache", "logging", xs @ _*) => MergeStrategy.first
      case PathList("org", "slf4j", xs @ _*)             => MergeStrategy.first
      case "application.conf"                            => MergeStrategy.concat
      case "reference.conf"                              => MergeStrategy.concat
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

lazy val hub = (project in file("hub"))
  .dependsOn(online, service_commons, spark)
  .settings(
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    assembly / artifact := {
      val art = (assembly / artifact).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(assembly / artifact, assembly),
    libraryDependencies ++= vertx_java,
    libraryDependencies ++= circe,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackClassicVersion,
      "org.slf4j" % "slf4j-api" % slf4jApiVersion,
      "com.typesafe" % "config" % "1.4.3",
      // force netty versions -> without this we conflict with the versions pulled in from
      // our online module's spark deps which causes the web-app to not serve up content
      "io.netty" % "netty-all" % "4.1.111.Final",
      // wire up metrics using micro meter and statsd
      "io.micrometer" % "micrometer-registry-statsd" % "1.13.6",
      // need this to prevent a NoClassDef error on org/json4s/Formats
      "org.json4s" %% "json4s-core" % "3.7.0-M11",
      "junit" % "junit" % "4.13.2" % Test,
      "com.novocode" % "junit-interface" % "0.11" % Test,
      "org.mockito" % "mockito-core" % "5.12.0" % Test,
      "io.vertx" % "vertx-unit" % vertxVersion % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % "test",
      "io.vertx" % "vertx-unit" % "4.5.10" % Test
    ),
    // Assembly settings
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    // Main class configuration
    // We use a custom launcher to help us wire up our statsd metrics
    Compile / mainClass := Some("ai.chronon.service.ChrononServiceLauncher"),
    assembly / mainClass := Some("ai.chronon.service.ChrononServiceLauncher"),
    // Merge strategy for assembly
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF")           => MergeStrategy.discard
      case PathList("META-INF", xs @ _*)                 => MergeStrategy.first
      case PathList("javax", "activation", xs @ _*)      => MergeStrategy.first
      case PathList("org", "apache", "logging", xs @ _*) => MergeStrategy.first
      case PathList("org", "slf4j", xs @ _*)             => MergeStrategy.first
      case "application.conf"                            => MergeStrategy.concat
      case "reference.conf"                              => MergeStrategy.concat
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", _*)            => MergeStrategy.filterDistinctLines
  case "plugin.xml"                        => MergeStrategy.last
  case PathList("com", "fasterxml", _*)    => MergeStrategy.last
  case PathList("com", "google", _*)       => MergeStrategy.last
  case _                                   => MergeStrategy.first
}
exportJars := true
