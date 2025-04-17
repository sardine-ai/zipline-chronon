import os
import sys

import click

from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.compiler import Compiler
from ai.chronon.cli.compile.display.console import console


@click.command(name="compile")
@click.option(
    "--chronon-root",
    envvar="CHRONON_ROOT",
    help="Path to the root chronon folder",
    default=os.getcwd(),
)
def compile_v3(chronon_root):

    print()

    if chronon_root not in sys.path:
        console.print(
            f"Adding [cyan italic]{chronon_root}[/cyan italic] to python path, during compile."
        )
        sys.path.append(chronon_root)
    else:
        console.print(
            f"[cyan italic]{chronon_root}[/cyan italic] already on python path."
        )

    return __compile_v3(chronon_root)


def __compile_v3(chronon_root):
    if chronon_root:
        chronon_root_path = os.path.expanduser(chronon_root)
        os.chdir(chronon_root_path)

    # check that a "teams.py" file exists in the current directory
    if not (os.path.exists("teams.py") or os.path.exists("teams.json")):
        raise click.ClickException(
            (
                "teams.py or teams.json file not found in current directory."
                " Please run from the top level of conf directory."
            )
        )

    compile_context = CompileContext()
    compiler = Compiler(compile_context)
    results = compiler.compile()
    return results


if __name__ == "__main__":
    compile_v3()
