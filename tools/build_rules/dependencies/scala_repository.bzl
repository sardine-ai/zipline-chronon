load("@io_bazel_rules_scala_config//:config.bzl", "SCALA_MAJOR_VERSION", "SCALA_VERSION")
load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repository", "versioned_artifacts")

SCALA_REPOSITORY_NAME = "scala"

# We dynamically pull the right scala version based on config so these artifacts cannot be pinned with other maven
# artifacts and we need a separate repository
scala_repository = repository(
    name = SCALA_REPOSITORY_NAME,
    pinned = False,
    maven_install_json = None,
    artifacts = [
        "org.scala-lang:scala-library:" + SCALA_VERSION,
        "org.scala-lang:scala-reflect:" + SCALA_VERSION,
    ],
    excluded_artifacts = [],
    overrides = {},
)
