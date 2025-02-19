DEFAULT_SCALA_VERSION = "2.12.18"

def _scala_version_impl(repository_ctx):
    scala_version = repository_ctx.os.environ.get("SCALA_VERSION", DEFAULT_SCALA_VERSION)
    repository_ctx.file("BUILD", "")
    repository_ctx.file("version.bzl",
        content = "SCALA_VERSION = '%s'\n" % scala_version)

scala_version = repository_rule(
    implementation = _scala_version_impl,
    environ = ["SCALA_VERSION"],
)