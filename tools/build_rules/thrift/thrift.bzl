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
    thrift_binary = ctx.attr.thrift_binary
    root = ctx.actions.declare_directory("gen_thrift")
    all_outputs = [root]

    commands = ["mkdir -p {root_dir}".format(root_dir = root.path)]

    for src in ctx.files.srcs:
        # Declare output directory matching the namespace structure
        output_dir = "gen_thrift/" + src.basename.replace(".thrift", "")
        constants_py = ctx.actions.declare_file("{}/constants.py".format(output_dir))
        ttypes_py = ctx.actions.declare_file("{}/ttypes.py".format(output_dir))
        module_init = ctx.actions.declare_file("{}/__init__.py".format(output_dir))

        file_outputs = [constants_py, ttypes_py, module_init]
        all_outputs.extend(file_outputs)

        # Command to generate files in the correct namespace
        command = "{thrift_binary} --gen py -out $(dirname {output_dir}) {src}".format(
            thrift_binary = thrift_binary,
            namespace = ctx.attr.namespace,
            output_dir = module_init.dirname,
            src = src.path
        )
        commands.append(command)

    combined_command = " && ".join(commands)

    print("commands: {}".format(combined_command))

    # Generate files
    ctx.actions.run_shell(
        outputs = all_outputs,
        inputs = ctx.files.srcs,
        command = combined_command,
        progress_message = "Generating Python code from Thrift files: %s" % ", ".join([src.path for src in ctx.files.srcs]),
    )

    return [DefaultInfo(files = depset(all_outputs))]

_thrift_python_library_gen = rule(
    implementation = _thrift_python_library_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = [".thrift"]),
        "thrift_binary": attr.string(),
        "namespace": attr.string(),
    },
)

def thrift_python_library(name, srcs, visibility = None):
    """Generates Python code from Thrift files"""
    _thrift_python_library_gen(
        name = name + "_gen",
        srcs = srcs,
        thrift_binary = select({
            "@platforms//os:macos": "/opt/homebrew/bin/thrift",
            "//conditions:default": "/usr/local/bin/thrift",
        }),
    )

    py_library(
        name = name,
        srcs = [":" + name + "_gen"],
        # "gen_thrift/" needed for relative imports within the generated files
        imports = [".", "gen_thrift/"],
        visibility = visibility,
    )