load("@rules_jvm_external//:defs.bzl", _rje_artifact = "artifact")
load("//tools/build_rules:jar_library.bzl", "jar_library")
load("@io_bazel_rules_scala_config//:config.bzl", "SCALA_MAJOR_VERSION", "SCALA_VERSION")
load("//tools/build_rules/dependencies:maven_repository.bzl", "MAVEN_REPOSITORY_NAME")
load("//tools/build_rules/dependencies:spark_repository.bzl", "SPARK_REPOSITORY_NAME")

def _safe_name(coord):
    return coord.replace(":", "_").replace(".", "_").replace("-", "_")

def _get_artifact(coord, repository_name):
    """
    Helper macro to translate Maven coordinates into Bazel deps. Example:
    java_library(
        name = "foo",
        srcs = ["Foo.java"],
        deps = [maven_artifact("com.google.guava:guava")],
    )
    Arguments:
        repository_name: If provided, always fetch from this Maven repo instead of determining
        the repo automatically. Be careful when using this as Bazel will not prevent multiple
        jars from providing the same class on the classpath, in which case the order of "deps"
        will determine which one "wins".
    """
    if repository_name:
        return _rje_artifact(coord, repository_name = repository_name)

    safe_name = _safe_name(coord)

    if not native.existing_rule(safe_name):
        jar_library(
            name = safe_name,
            jars = [coord],
            visibility = ["//visibility:private"],
            tags = ["manual"],
        )
    return safe_name

# For specifying dependencies pulled from Maven Repository in our build targets
# Example: maven_artifact("com.google.guava:guava")
def maven_artifact(coord):
    return _get_artifact(coord, MAVEN_REPOSITORY_NAME)

# For specifying scala related dependencies pulled from Maven Repository in our build targets
# Example: maven_scala_artifact("org.rogach:scallop"),
def maven_scala_artifact(coord):
    """
    Same as "maven_artifact" but appends the current Scala version to the Maven coordinate.
    """
    full_coord = coord + "_" + SCALA_MAJOR_VERSION
    return _get_artifact(full_coord, MAVEN_REPOSITORY_NAME)

# For specifying dependencies pulled from Spark Repository in our build targets
# Example: maven_artifact("com.google.guava:guava")
def spark_artifact(coord):
    return _get_artifact(coord, SPARK_REPOSITORY_NAME)

