import click

from ai.chronon.repo.compilev3 import compile_v3
from ai.chronon.repo.init import main as init_main
from ai.chronon.repo.run import main as run_main


@click.group()
def zipline():
    pass


zipline.add_command(compile_v3)
zipline.add_command(run_main)
zipline.add_command(init_main)
