import click
from datetime import datetime, timedelta
import subprocess

from ai.chronon.cli.compile.compiler import Compiler
from ai.chronon.cli.compile.compile_context import CompileContext


def get_current_branch():
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"])
            .decode("utf-8")
            .strip()
        )
    except:
        return "main"


@click.group()
def cli():
    """Zipline CLI tool for sync, backfill and deploy operations"""
    pass


@cli.command()
@click.option("--branch", default=get_current_branch, help="Branch to sync")
def sync(branch):
    """Sync data for the specified branch"""
    click.echo(f"Syncing data for {branch} branch")
    compile_context = CompileContext()
    compiler = Compiler(compile_context)
    compiler.compile(compile_context)


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
@click.option(
    "--scope",
    type=click.Choice(["upstream", "self", "downstream"]),
    default="upstream",
    help="Scope of configs to backfill",
)
def backfill(conf: str, start_date: str, end_date: str, scope: str):
    """Backfill data between start and end dates"""
    click.echo(
        f"Backfilling with scope {scope} for config {conf} from {start_date} to {end_date}"
    )


@cli.command()
@click.argument("conf")
@click.option("--branch", default=get_current_branch, help="Target branch")
def deploy(conf: str, branch: str):
    """Deploy to specified branch"""
    click.echo(f"Deploying to {branch} using config {conf}")


if __name__ == "__main__":
    cli()
