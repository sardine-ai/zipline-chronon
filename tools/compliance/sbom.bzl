# This file is copied from the bazel github repository https://github.com/bazelbuild/bazel/blob/master/tools/compliance
# as it was recently added in latest bazel version but we are currently using old 5.4.0 because of pending
# bazelmod migration for rules_scala package
# *** DO NOT MODIFY THIS FILE UNLESS WE REALLY NEED TO ***

"""Generate an SBOM for a target."""

load(":gather_packages.bzl", "packages_used")

def _sbom_impl(ctx):
    # Gather all licenses and write information to one place

    # Now turn the big blob of data into something consumable.
    outputs = [ctx.outputs.out]
    args = ctx.actions.args()
    inputs = [ctx.file.packages_used]
    args.add("--packages_used", ctx.file.packages_used.path)
    args.add("--out", ctx.outputs.out.path)
    if ctx.attr.maven_install:
        args.add("--maven_install", ctx.file.maven_install.path)
        inputs.append(ctx.file.maven_install)
    ctx.actions.run(
        mnemonic = "CreateSBOM",
        progress_message = "Creating SBOM for %s" % ctx.label,
        inputs = inputs,
        outputs = outputs,
        executable = ctx.executable._sbom_generator,
        arguments = [args],
    )
    return [DefaultInfo(files = depset(outputs))]

_sbom = rule(
    implementation = _sbom_impl,
    attrs = {
        "packages_used": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "out": attr.output(mandatory = True),
        "_sbom_generator": attr.label(
            default = Label("//tools/compliance:write_sbom_private"),
            executable = True,
            allow_files = True,
            cfg = "exec",
        ),
        "maven_install": attr.label(
            mandatory = False,
            allow_single_file = True,
        ),
    },
)

def sbom(
        name,
        target,
        out = None,
        maven_install = "//:maven_install.json"):
    """Wrapper for sbom rule.

    Args:
        name: name
        target: Target to create sbom for
        out: output file name
        maven_install: maven lock file
    """
    packages = "_packages_" + name
    packages_used(
        name = packages,
        target = target,
        out = packages + ".json",
    )
    if not out:
        out = name + "_sbom.json"
    _sbom(
        name = name,
        out = out,
        packages_used = ":" + packages + ".json",
        maven_install = maven_install,
    )
