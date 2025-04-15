#!/usr/bin/env python

import os
import shutil

import click
from importlib_resources import files


@click.command(name="init")
@click.option(
    "--cloud_provider",
    envvar="CLOUD_PROVIDER",
    help="Cloud provider to use.",
    required=True,
    type=click.Choice(['aws', 'gcp'], case_sensitive=False)
)
@click.option(
    "--chronon_root",
    envvar="CHRONON_ROOT",
    help="Path to the root chronon folder.",
    default=os.path.join(os.getcwd(), "zipline"),
    type=click.Path(file_okay=False, writable=True),
)
@click.pass_context
def main(ctx, chronon_root, cloud_provider):
    try:
        template_path = files("ai.chronon").joinpath("resources", cloud_provider.lower())
    except AttributeError:
        click.echo("Error: Template directory not found in package.", err=True)
        return

    if not template_path.exists():
        click.echo("Error: Template directory not found in package.", err=True)
        return

    target_path = os.path.abspath(chronon_root)

    # Prevent accidental overwrites
    if os.path.exists(target_path) and os.listdir(target_path):
        click.confirm(f"Warning: {target_path} is not empty. Proceed?", abort=True)

    click.echo(f"Generating scaffolding at {target_path}...")

    try:
        shutil.copytree(template_path, target_path, dirs_exist_ok=True)
        click.echo("Project scaffolding created successfully! 🎉")
        click.echo(
            f""""Please copy the following command to your shell config:
    `export PYTHONPATH={target_path}:$PYTHONPATH`"""
        )
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


if __name__ == "__main__":
    main()
