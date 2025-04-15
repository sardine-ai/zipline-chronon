load(":gather_packages.bzl", "packages_used")
load(":sbom.bzl", "sbom")

packages_used(
    name = "zipline_packages",
    out = "zipline_packages.json",
    target = "//:zipline_packages_deploy.jar",
)

sbom(
    name = "zipline_sbom",
    out = "zipline_sbom.json",
    target = "//:zipline_packages_deploy.jar",
)

# Add the write_sbom_private target similar to the reference
py_binary(
    name = "write_sbom_private",
    srcs = ["write_sbom.py"],
    main = "write_sbom.py",
)
