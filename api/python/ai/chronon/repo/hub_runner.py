import os

import click

from ai.chronon.repo import hub_utils, utils
from ai.chronon.repo.constants import RunMode
from ai.chronon.repo.zipline_hub import ZiplineHub


@click.group()
def hub():
    pass

#### Common click options
def common_options(func):
    func = click.option("--repo", help="Path to chronon repo", default=".")(func)
    func = click.option("--conf", required=True, help="Conf param - required for every mode")(func)
    func = click.option("--hub_url", help="The URL of the Zipline hub to use", envvar="ZIPLINE_HUB")(func)
    return func

def ds_option(func):
    return click.option("--ds", help="the end partition to backfill the data")(func)

def start_ds_option(func):
    return click.option(
    "--start-ds",
    help="override the original start partition for a range backfill. "
    "It only supports staging query, group by backfill and join jobs. "
    "It could leave holes in your final output table due to the override date range.",)(func)

def end_ds_option(func):
    return click.option("--end-ds", help="the end ds for a range backfill")(func)


# zipline hub backfill --conf=compiled/joins/join
# adhoc backfills
@hub.command()
@common_options
@start_ds_option
@end_ds_option
def backfill(repo,
             hub_url,
             conf,
             # mode,
             start_ds,
             end_ds):
    """
    - Call diff API to get the diff of the repo.
    Response should contain a list of confs that are different from what's on remote.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual run API
    """
    zipline_hub = ZiplineHub(base_url=hub_url)
    hub_utils.upload_to_branch(chronon_root=repo, zipline_hub=zipline_hub)

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)
    zipline_hub.call_workflow_start_api(
        conf_name=conf_name,
        mode=RunMode.BACKFILL.value,
        branch=hub_utils.get_branch(repo),  # Get the current branch
        user=os.environ.get('USER'),
        start=start_ds,
        end=end_ds
    )

if __name__ == "__main__":
    hub()


