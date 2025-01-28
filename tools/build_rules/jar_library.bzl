load("@bazel_skylib//lib:dicts.bzl", "dicts")
load("@rules_jvm_external//:defs.bzl", "artifact")

DEFAULT_PROVIDED_REPO = "maven"  # For backwards compatability

def jar_library(name, jars = [], overrides = {}, visibility = ["//visibility:public"], **kwargs):

    def _get_jars(repo_name):
        return [artifact(jar, repository_name = repo_name) for jar in jars]

    repo_name = DEFAULT_PROVIDED_REPO
    configured_jars = _get_jars(repo_name)

    native.java_library(
        name = name,
        exports = configured_jars,
        visibility = visibility,
        **kwargs
    )