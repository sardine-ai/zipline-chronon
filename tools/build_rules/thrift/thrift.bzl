load("@rules_python//python:defs.bzl", "py_library")

# Generates java files from the input thrift files
# Thrift gen command only supports single input file so we are creating actions separately for each thrift file
def generate_java_files_using_thrift(ctx):
    thrift_path = ctx.attr.thrift_binary
    output_directories = []

    for src_file in ctx.files.srcs:
        # Generate unique output directory for each thrift file
        # This is necessary to run all the below actions in parallel using bazel
        output_directory = ctx.actions.declare_directory(
            ctx.label.name + "_" + src_file.basename.replace(".thrift", ""),
        )
        output_directories.append(output_directory)

        # Create action for each thrift file separately
        ctx.actions.run(
            outputs = [output_directory],
            inputs = [src_file],
            executable = thrift_path,
            arguments = [
                "--gen",
                "java:generated_annotations=undated",
                "-out",
                output_directory.path,
                src_file.path,
            ],
            progress_message = "Generating Java code from %s file" % src_file.path,
        )

    return output_directories

# Creates jar file including all files from the given input directories
def create_jar_file(ctx, input_directories):
    jar_file = ctx.actions.declare_file(ctx.label.name + ".srcjar")

    jar_cmds = ["jar cf " + jar_file.path]
    for input_directory in input_directories:
        jar_cmds.append("-C " + input_directory.path + " .")
    jar_cmd = " ".join(jar_cmds)

    ctx.actions.run_shell(
        outputs = [jar_file],
        inputs = input_directories,
        command = jar_cmd,
        progress_message = "Creating srcjar from all input files",
    )

    return jar_file

def _thrift_gen_library_impl(ctx):
    thrift_output_directories = generate_java_files_using_thrift(ctx)
    final_output_directories = replace_java_files_with_custom_thrift_package_prefix(ctx, thrift_output_directories)
    jar_file = create_jar_file(ctx, final_output_directories)

    return [DefaultInfo(files = depset([jar_file]))]

def replace_java_files_with_custom_thrift_package_prefix(ctx, input_directories):
    output_directories = []
    script = ctx.executable._python_script
    for input_directory in input_directories:
        output_directory = ctx.actions.declare_directory(
            input_directory.basename + "_modified"
        )
        output_directories.append(output_directory)

        ctx.actions.run(
            executable=script,
            inputs = [input_directory],
            outputs = [output_directory],
            arguments = [
                "-v",
                input_directory.path,
                output_directory.path
            ],
            progress_message = "Replacing package names in input Java files for %s" % input_directory.short_path,
        )

    return output_directories

_thrift_java_library = rule(
    implementation = _thrift_gen_library_impl,
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".thrift"],
            mandatory = True,
            doc = "List of .thrift source files",
        ),
        "thrift_binary": attr.string(),
        "_python_script": attr.label(
            default = "//scripts/codemod:thrift_package_replace",
            executable = True,
            cfg = "host",
        ),
    },
)

# Currently only supports java files generation
# TODO: To make it more generic for handling other languages
def thrift_java_library(name, srcs, **kwargs):
    _thrift_java_library(
        name = name,
        srcs = srcs,
        thrift_binary = select({
            "@platforms//os:macos": "/opt/homebrew/bin/thrift",
            "//conditions:default": "/usr/local/bin/thrift",
        }),
        **kwargs
    )



def _thrift_python_library_impl(ctx):

    all_outputs = []

    for src in ctx.files.srcs:
        rule_name = ctx.label.name
        # Get base name without .thrift extension
        base_name = src.basename.replace(".thrift", "")

        # Convert namespace to directory structure
        namespace_dir = ctx.attr.namespace.replace(".", "/")

        # Declare output directory matching the namespace structure
        output_dir = "{}/{}/{}".format(rule_name, namespace_dir, base_name)
        constants_py = ctx.actions.declare_file("{}/constants.py".format(output_dir))
        ttypes_py = ctx.actions.declare_file("{}/ttypes.py".format(output_dir))

        file_outputs = [constants_py, ttypes_py]
        all_outputs.extend(file_outputs)

        ctx.actions.run(
            outputs = [output_dir],
            inputs = [src],
            executable = ctx.attr.thrift_binary,
            arguments = [
                "--gen",
                "py",
                "-out",
                output_dir.path,
                src.path,
            ],
            progress_message = "Generating Python code from %s file" % src.path,
        )

    return [DefaultInfo(files = depset(all_outputs))]

_thrift_python_library = rule(
    implementation = _thrift_python_library_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = [".thrift"]),
        "thrift_binary": attr.string(),
        "namespace": attr.string(),
    },
)

def thrift_python_library(name, srcs, namespace, visibility = None):
    """Generates Python code from Thrift files with correct namespace structure."""
    _thrift_python_library(
        name = name + "_gen",
        srcs = srcs,
        namespace = namespace,
        thrift_binary = select({
            "@platforms//os:macos": "/opt/homebrew/bin/thrift",
            "//conditions:default": "/usr/local/bin/thrift",
        }),
    )

    py_library(
        name = name,
        srcs = [":" + name + "_gen"],
        imports = ["."],
        visibility = visibility,
    )