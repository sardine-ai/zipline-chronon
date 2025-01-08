load("@rules_jvm_external//:defs.bzl", "DEFAULT_REPOSITORY_NAME")
load("@rules_jvm_external//:specs.bzl", "json", "maven", "parse")

# Repository artifacts are defined in external files
load(":maven_repository.bzl", "maven_repository")
load(":spark_repository.bzl", "spark_repository")

all_repositories = [
    # The main repositories are defined in individual files, which are loaded above and referenced here
    maven_repository,
    spark_repository,
]

def get_repository(repository_name):
    for repository in all_repositories:
        if repository.name == repository_name:
            return repository
    return None