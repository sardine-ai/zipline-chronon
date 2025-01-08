# Generates java files from the input thrift files
# Thrift gen command only supports single input file so we are creating actions separately for each thrift file
def generate_java_files_using_thrift(ctx):
    thrift_path = ctx.attr.thrift_binary
    output_directories = []

    for src_file in ctx.files.srcs:
        # Generate unique output directory for each thrift file
        # This is necessary to run all the below actions in parallel using bazel
        output_directory = ctx.actions.declare_directory(
            ctx.label.name + "_" + src_file.basename.replace(".thrift", "")
        )
        output_directories.append(output_directory)

        # Create action for each thrift file separately
        ctx.actions.run(
            outputs = [output_directory],
            inputs = [src_file],
            executable = thrift_path,
            arguments = [
                "-strict",
                "--gen", "java:generated_annotations=undated",
                "-out", output_directory.path,
                src_file.path
            ],
            progress_message = "Generating Java code from %s file" % src_file.path,
        )

    return output_directories

# This is necessary as we have custom thrift dependency in `ai.chronon.api.thrift` package
def replace_java_files_with_custom_thrift_package_prefix(ctx, input_directories):
    output_directories = []
    for input_directory in input_directories:
        # Declare the output directory with modified files
        output_directory = ctx.actions.declare_directory(
            input_directory.basename + "_modified"
        )
        output_directories.append(output_directory)

        replace_command = """
            shopt -s globstar
            for input_file in {input_path}/**/*.java
            do
                output_file={output_path}/$(basename $input_file)
                sed 's/org.apache.thrift/ai.chronon.api.thrift/g' $input_file > $output_file
            done
        """.format(input_path=input_directory.path, output_path=output_directory.path)

        ctx.actions.run_shell(
            inputs=[input_directory],
            outputs=[output_directory],
            command=replace_command,
            progress_message="Replacing package names in input Java files for %s" % input_directory.short_path,
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
        outputs=[jar_file],
        inputs=input_directories,
        command=jar_cmd,
        progress_message="Creating srcjar from all input files",
    )

    return jar_file

def _thrift_java_library_impl(ctx):
    thrift_output_directories = generate_java_files_using_thrift(ctx)
    final_output_directories = replace_java_files_with_custom_thrift_package_prefix(ctx, thrift_output_directories)
    jar_file = create_jar_file(ctx, final_output_directories)

    return [DefaultInfo(files = depset([jar_file]))]

_thrift_java_library = rule(
    implementation = _thrift_java_library_impl,
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".thrift"],
            mandatory = True,
            doc = "List of .thrift source files",
        ),
        "thrift_binary": attr.string(),
    },
)

def thrift_java_library(name, srcs, **kwargs):
    _thrift_java_library(
        name = name,
        srcs = srcs,
        thrift_binary = select({
            "@platforms//os:linux": "/usr/local/bin/thrift",
            "@platforms//os:macos": "/opt/homebrew/bin/thrift",
            "//conditions:default": "/usr/local/bin/thrift",
        }),
        **kwargs
    )