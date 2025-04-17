load("@bazel_skylib//lib:dicts.bzl", "dicts")
load("@rules_jvm_external//:defs.bzl", "artifact", "maven_install")
load(":all_repositories.bzl", "all_repositories")

_repository_urls = [
    "https://repo1.maven.org/maven2/",
    "https://packages.confluent.io/maven/",
    "https://linkedin.jfrog.io/artifactory/avro-util/",
]

def load_all_dependencies():
    for repository in all_repositories:
        maven_install(
            name = repository.name,
            artifacts = repository.artifacts,
            repositories = _repository_urls,
            fetch_sources = True,
            version_conflict_policy = "pinned",
            duplicate_version_warning = "error",
            fail_if_repin_required = True,
            resolve_timeout = 5000,
            maven_install_json = repository.maven_install_json,
            excluded_artifacts = repository.excluded_artifacts,
        )
