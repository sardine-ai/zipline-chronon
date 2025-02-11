workspace(name = "chronon")

# Scala version used across the project
SCALA_VERSION = "2.12.18"

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Contains useful bazel utility functions and rules
http_archive(
    name = "bazel_skylib",
    sha256 = "bc283cdfcd526a52c3201279cda4bc298652efa898b10b4db0837dc51652756f",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz",
    ],
)

# For Java support
http_archive(
    name = "rules_java",
    sha256 = "e81e9deaae0d9d99ef3dd5f6c1b32338447fe16d5564155531ea4eb7ef38854b",
    urls = [
        "https://github.com/bazelbuild/rules_java/releases/download/7.0.6/rules_java-7.0.6.tar.gz",
    ],
)

load("@rules_java//java:repositories.bzl", "remote_jdk17_repos", "rules_java_dependencies", "rules_java_toolchains")

rules_java_dependencies()

rules_java_toolchains()

remote_jdk17_repos()

# For JVM support
http_archive(
    name = "rules_jvm_external",
    sha256 = "3afe5195069bd379373528899c03a3072f568d33bd96fe037bd43b1f590535e7",
    strip_prefix = "rules_jvm_external-6.6",
    url = "https://github.com/bazel-contrib/rules_jvm_external/releases/download/6.6/rules_jvm_external-6.6.tar.gz",
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

# For additional rulesets like java_test_suite
http_archive(
    name = "contrib_rules_jvm",
    sha256 = "2412e22bc1eb9d3a5eae15180f304140f1aad3f8184dbd99c845fafde0964559",
    strip_prefix = "rules_jvm-0.24.0",
    urls = ["https://github.com/bazel-contrib/rules_jvm/releases/download/v0.24.0/rules_jvm-v0.24.0.tar.gz"],
)

load("@contrib_rules_jvm//:repositories.bzl", "contrib_rules_jvm_deps")

contrib_rules_jvm_deps()

load("@contrib_rules_jvm//:setup.bzl", "contrib_rules_jvm_setup")

contrib_rules_jvm_setup()

# For Scala support
http_archive(
    name = "io_bazel_rules_scala",
    sha256 = "e734eef95cf26c0171566bdc24d83bd82bdaf8ca7873bec6ce9b0d524bdaf05d",
    strip_prefix = "rules_scala-6.6.0",
    url = "https://github.com/bazelbuild/rules_scala/releases/download/v6.6.0/rules_scala-v6.6.0.tar.gz",
)

# Initialize Scala with specific version support
load("@io_bazel_rules_scala//:scala_config.bzl", "scala_config")

scala_config(scala_version = SCALA_VERSION)

load("@io_bazel_rules_scala//scala:scala_maven_import_external.bzl", "scala_maven_import_external")

scala_maven_import_external(
    name = "scala_compiler_source_2_12_18",
    artifact = "org.scala-lang:scala-compiler:%s:sources" % SCALA_VERSION,
    artifact_sha256 = "f79ee80f140218253f2a38c9d73f8a9b552d06afce7a5f61cf08079a388e21df",
    licenses = ["notice"],
    server_urls = [
        "https://repo1.maven.org/maven2",
        "https://mirror.bazel.build/repo1.maven.org/maven2",
    ],
)

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")
scala_repositories()

load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")
scala_register_toolchains()

load("@io_bazel_rules_scala//testing:scalatest.bzl", "scalatest_repositories", "scalatest_toolchain")
scalatest_repositories()
scalatest_toolchain()

# For scalafmt
load("@io_bazel_rules_scala//scala/scalafmt:scalafmt_repositories.bzl", "scalafmt_default_config", "scalafmt_repositories")
scalafmt_default_config()
scalafmt_repositories()

# For Protobuf support
http_archive(
    name = "rules_proto",
    sha256 = "dc3fb206a2cb3441b485eb1e423165b231235a1ea9b031b4433cf7bc1fa460dd",
    strip_prefix = "rules_proto-5.3.0-21.7",
    urls = [
        "https://github.com/bazelbuild/rules_proto/archive/refs/tags/5.3.0-21.7.tar.gz",
    ],
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

# To load all dependencies used across our modules
load("//tools/build_rules/dependencies:load_dependencies.bzl", "load_all_dependencies")

load_all_dependencies()
