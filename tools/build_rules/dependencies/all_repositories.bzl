load("@io_bazel_rules_scala_config//:config.bzl", "SCALA_MAJOR_VERSION", "SCALA_VERSION")
load("@rules_jvm_external//:specs.bzl", "json", "maven", "parse")

# Repository artifacts are defined in external files
load(":maven_repository.bzl", "maven_repository")
load(":scala_repository.bzl", "scala_repository")

all_repositories = [
    # The main repositories are defined in individual files, which are loaded above and referenced here
    maven_repository,
    scala_repository,
]

def get_repository(repository_name):
    for repository in all_repositories:
        if repository.name == repository_name:
            return repository
    return None
