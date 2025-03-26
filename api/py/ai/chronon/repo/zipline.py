import click

from ai.chronon.repo.compile import extract_and_convert
from ai.chronon.repo.init import main as init_main
from ai.chronon.repo.run import main as run_main


@click.group()
def zipline():
    pass


zipline.add_command(extract_and_convert)
zipline.add_command(run_main)
zipline.add_command(init_main)
