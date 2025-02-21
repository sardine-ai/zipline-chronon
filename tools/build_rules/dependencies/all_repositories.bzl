load("@rules_jvm_external//:defs.bzl", "DEFAULT_REPOSITORY_NAME")
load("@rules_jvm_external//:specs.bzl", "json", "maven", "parse")
load("@io_bazel_rules_scala_config//:config.bzl", "SCALA_MAJOR_VERSION", "SCALA_VERSION")

# Repository artifacts are defined in external files
load(":maven_repository.bzl", "maven_repository")
load(":spark_repository.bzl", "spark_repository")
load(":scala_2_12_repository.bzl", "scala_2_12_repository")
load(":scala_2_13_repository.bzl", "scala_2_13_repository")

scala_repository = (scala_2_12_repository if SCALA_MAJOR_VERSION == "2.12" else scala_2_13_repository)

all_repositories = [
    # The main repositories are defined in individual files, which are loaded above and referenced here
    maven_repository,
    spark_repository,
    scala_repository,
]

def get_repository(repository_name):
    for repository in all_repositories:
        if repository.name == repository_name:
            return repository
    return None