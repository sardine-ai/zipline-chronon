import os

import click

from ai.chronon.repo import hub_uploader, hub_utils, utils
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
    - Submit a backfill job to Zipline.
    Response should contain a list of confs that are different from what's on remote.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual run API with mode set to backfill.
    """
    zipline_hub = ZiplineHub(base_url=hub_url)
    conf_name_to_hash_dict = hub_uploader.build_local_repo_hashmap(root_dir= repo)
    hub_utils.upload_to_branch(chronon_root=repo, zipline_hub=zipline_hub, local_repo_entities= conf_name_to_hash_dict)

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)
    response_json = zipline_hub.call_workflow_start_api(
        conf_name=conf_name,
        mode=RunMode.BACKFILL.value,
        branch=hub_utils.get_branch(repo),  # Get the current branch
        user=os.environ.get('USER'),
        start=start_ds,
        end=end_ds,
        conf_hash=conf_name_to_hash_dict[conf_name].hash,
    )

    print(response_json)

# zipline hub deploy --conf=compiled/joins/join
# currently only supports one-off deploy node submission
@hub.command()
@common_options
@end_ds_option
def deploy(repo,
           hub_url,
           conf,
           end_ds):
    """
    - Submit a one-off deploy job to Zipline.
    Response should contain a list of confs that are different from what's on remote.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual run API with mode set to deploy
    """
    zipline_hub = ZiplineHub(base_url=hub_url)
    conf_name_to_hash_dict = hub_uploader.build_local_repo_hashmap(root_dir= repo)

    hub_utils.upload_to_branch(chronon_root=repo, zipline_hub=zipline_hub, local_repo_entities= conf_name_to_hash_dict)

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)
    response_json = zipline_hub.call_workflow_start_api(
        conf_name=conf_name,
        mode=RunMode.DEPLOY.value,
        branch=hub_utils.get_branch(repo),  # Get the current branch
        user=os.environ.get('USER'),
        start=end_ds,  # Deploy covers just 1 day, so we use end_ds as start
        end=end_ds,
        conf_hash=conf_name_to_hash_dict[conf_name].hash,
    )

    print(response_json)

if __name__ == "__main__":
    hub()


