from typing import Dict
import click
from datetime import datetime, timedelta

from ai.chronon.api.common.ttypes import ConfigType
from ai.chronon.cli.plan.physical_index import (
    PhysicalIndex,
    get_backfill_physical_graph,
    submit_physical_graph,
)
from ai.chronon.cli.compile.compiler import CompileResult, Compiler
from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.git_utils import get_current_branch


@click.group()
def cli():
    """Zipline CLI tool for sync, backfill and deploy operations"""
    pass


def compile() -> Dict[ConfigType, CompileResult]:
    compile_context = CompileContext()
    compiler = Compiler(compile_context)
    # TODO(orc): add column lineage to objects
    return compiler.compile(compile_context)


@cli.command()
@click.argument("conf")
@click.option(
    "--start-date",
    default=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
    help="Start date for backfill (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    default=datetime.now().strftime("%Y-%m-%d"),
    help="End date for backfill (YYYY-MM-DD)",
)
@click.option("--branch", default=get_current_branch, help="Branch to sync")
def backfill(conf: str, start_date: str, end_date: str):
    # compile
    compile_result = compile()
    physical_index = PhysicalIndex.from_compiled_obj(compile_result)
    physical_graph = get_backfill_physical_graph(
        conf, physical_index, start_date, end_date
    )
    submit_physical_graph(physical_graph)


@cli.command()
@click.argument("conf")
@click.option("--branch", default=get_current_branch, help="Target branch")
def deploy(conf: str, branch: str):
    """Deploy to specified branch"""
    click.echo(f"Deploying to {branch} using config {conf}")


if __name__ == "__main__":
    cli()
