#!/usr/bin/env python3
"""
run.py needs to only depend in python standard library to simplify execution requirements.
"""

#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import os
from datetime import datetime

import click

from ai.chronon.repo.aws import (
    ZIPLINE_AWS_JAR_DEFAULT,
    ZIPLINE_AWS_ONLINE_CLASS_DEFAULT,
    AwsRunner,
)
from ai.chronon.repo.constants import (
    APP_NAME_TEMPLATE,
    AWS,
    CLOUD_PROVIDER_KEYWORD,
    GCP,
    MODE_ARGS,
    ONLINE_CLASS_ARG,
    ONLINE_JAR_ARG,
    ONLINE_MODES,
    RENDER_INFO_DEFAULT_SCRIPT,
    ZIPLINE_DIRECTORY,
    RunMode,
)
from ai.chronon.repo.default_runner import Runner
from ai.chronon.repo.gcp import (
    ZIPLINE_GCP_JAR_DEFAULT,
    ZIPLINE_GCP_ONLINE_CLASS_DEFAULT,
    GcpRunner,
)
from ai.chronon.repo.utils import get_environ_arg, set_runtime_env_v3


# TODO: @davidhan - we should move these to all be in the defaults of the choice args
def set_defaults(ctx):
    """Set default values based on environment."""
    chronon_repo_path = os.environ.get("CHRONON_REPO_PATH", ".")
    today = datetime.today().strftime("%Y-%m-%d")

    obj = ctx.obj if ctx.obj is not None else dict()

    defaults = {
        "ds": today,  # TODO: this breaks if the partition column is not the same as yyyy-MM-dd.
        "app_name": os.environ.get("APP_NAME"),
        "online_jar": os.environ.get("CHRONON_ONLINE_JAR"),
        "repo": chronon_repo_path,
        "online_class": os.environ.get("CHRONON_ONLINE_CLASS"),
        "version": os.environ.get("VERSION") or obj.get("version"),
        "spark_version": os.environ.get("SPARK_VERSION", "2.4.0"),
        "spark_submit_path": os.path.join(chronon_repo_path, "scripts/spark_submit.sh"),
        "spark_streaming_submit_path": os.path.join(
            chronon_repo_path, "scripts/spark_streaming.sh"
        ),
        # NOTE: We don't want to ever call the fetch_online_jar.py script since we're working
        # on our internal zipline fork of the chronon repo
        # "online_jar_fetch": os.path.join(chronon_repo_path, "scripts/fetch_online_jar.py"),
        "online_args": os.environ.get("CHRONON_ONLINE_ARGS"),
        "chronon_jar": os.environ.get("CHRONON_DRIVER_JAR"),
        "list_apps": "python3 " + os.path.join(chronon_repo_path, "scripts/yarn_list.py"),
        "render_info": os.path.join(chronon_repo_path, RENDER_INFO_DEFAULT_SCRIPT),
        "project_conf": obj.get("project_conf"),
        "artifact_prefix": os.environ.get("ARTIFACT_PREFIX"),
        "flink_state_uri": os.environ.get("FLINK_STATE_URI"),
    }
    for key, value in defaults.items():
        if ctx.params.get(key) is None and value is not None:
            ctx.params[key] = value


def validate_flink_state(ctx, param, value):
    uri_schemes = ["gs://", "s3://"]
    if value and not any(value.startswith(scheme) for scheme in uri_schemes):
        raise click.BadParameter(f"Flink state uri must start with {uri_schemes}")
    return value


def validate_additional_jars(ctx, param, value):
    if value:
        jars = value.split(",")
        for jar in jars:
            if not jar.startswith(("gs://", "s3://")):
                raise click.BadParameter(f"Additional jars must start with gs://, s3://: {jar}")
    return value


@click.command(
    name="run",
    context_settings=dict(allow_extra_args=True, ignore_unknown_options=True),
)
@click.option(
    "--conf", required=True, help="Conf param - required for every mode"
)  # TODO: @davidhan - we should be able to infer this in the future
@click.option(
    "--env",
    required=False,
    default="dev",
    help="Running environment - default to be dev",
)
@click.option(
    "--mode", type=click.Choice([str(k) for k in MODE_ARGS.keys()]), default=str(RunMode.BACKFILL)
)
@click.option("--ds", help="the end partition to backfill the data")
@click.option("--app-name", help="app name. Default to {}".format(APP_NAME_TEMPLATE))
@click.option(
    "--start-ds",
    help="override the original start partition for a range backfill. "
    "It only supports staging query, group by backfill and join jobs. "
    "It could leave holes in your final output table due to the override date range.",
)
@click.option("--end-ds", help="the end ds for a range backfill")
@click.option(
    "--parallelism",
    help="break down the backfill range into this number of tasks in parallel. "
    "Please use it along with --start-ds and --end-ds and only in manual mode",
)
@click.option("--repo", help="Path to chronon repo", default=".")
@click.option(
    "--online-jar",
    help="Jar containing Online KvStore & Deserializer Impl. "
    "Used for streaming and metadata-upload mode.",
)
@click.option(
    "--online-class",
    help="Class name of Online Impl. Used for streaming and metadata-upload mode.",
)
@click.option("--version", required=False, help="Chronon version to use.")
@click.option("--spark-version", default="2.4.0", help="Spark version to use for downloading jar.")
@click.option("--spark-submit-path", help="Path to spark-submit")
@click.option("--spark-streaming-submit-path", help="Path to spark-submit for streaming")
@click.option(
    "--online-jar-fetch",
    help="Path to script that can pull online jar. This will run only "
    "when a file doesn't exist at location specified by online_jar",
)
@click.option("--sub-help", is_flag=True, help="print help command of the underlying jar and exit")
@click.option(
    "--conf-type",
    help="related to sub-help - no need to set unless you are not working with a conf",
)
@click.option("--online-args", help="Basic arguments that need to be supplied to all online modes")
@click.option("--chronon-jar", help="Path to chronon OS jar")
@click.option("--release-tag", help="Use the latest jar for a particular tag.")
@click.option("--list-apps", help="command/script to list running jobs on the scheduler")
@click.option(
    "--render-info",
    help="Path to script rendering additional information of the given config. "
    "Only applicable when mode is set to info",
)
@click.option("--kafka-bootstrap", help="Kafka bootstrap server in host:port format")
@click.option(
    "--latest-savepoint",
    is_flag=True,
    default=False,
    help="Deploys streaming job with latest savepoint",
)
@click.option("--custom-savepoint", help="Savepoint to deploy streaming job with.")
@click.option(
    "--no-savepoint", is_flag=True, default=False, help="Deploys streaming job without a savepoint"
)
@click.option(
    "--version-check",
    is_flag=True,
    default=False,
    help="Checks if Zipline version of running streaming job is different from local version and deploys the job if they are different",
)
@click.option(
    "--flink-state-uri",
    help="Bucket for storing flink state checkpoints/savepoints and other internal pieces for orchestration.",
    callback=validate_flink_state,
)
@click.option(
    "--additional-jars",
    help="Comma separated list of additional jar URIs to be included in the Flink job classpath (e.g. gs://bucket/jar1.jar,gs://bucket/jar2.jar).",
    callback=validate_additional_jars,
)
@click.option(
    "--validate",
    is_flag=True,
    help="Validate the catalyst util Spark expression evaluation logic",
)
@click.option("--validate-rows", default="10000", help="Number of rows to  run the validation on")
@click.option("--join-part-name", help="Name of the join part to use for join-part-job")
@click.option(
    "--artifact-prefix",
    help="Remote artifact URI to install zipline client artifacts necessary for interacting with Zipline infrastructure.",
)
@click.option("--disable-cloud-logging", is_flag=True, default=False, help="Disables cloud logging")
@click.option(
    "--enable-debug",
    is_flag=True,
    default=False,
    help="Enables verbose debug logging in run modes that support it",
)
@click.pass_context
def main(
    ctx,
    conf,
    env,
    mode,
    ds,
    app_name,
    start_ds,
    end_ds,
    parallelism,
    repo,
    online_jar,
    online_class,
    version,
    spark_version,
    spark_submit_path,
    spark_streaming_submit_path,
    online_jar_fetch,
    sub_help,
    conf_type,
    online_args,
    chronon_jar,
    release_tag,
    list_apps,
    render_info,
    kafka_bootstrap,
    latest_savepoint,
    custom_savepoint,
    no_savepoint,
    version_check,
    flink_state_uri,
    validate,
    validate_rows,
    join_part_name,
    artifact_prefix,
    disable_cloud_logging,
    additional_jars,
    enable_debug,
):
    unknown_args = ctx.args
    click.echo("Running with args: {}".format(ctx.params))

    conf_path = os.path.join(repo, conf)
    if not os.path.isfile(conf_path):
        raise ValueError(f"Conf file {conf_path} does not exist.")

    set_runtime_env_v3(ctx.params, conf)
    set_defaults(ctx)
    extra_args = (" " + online_args) if mode in ONLINE_MODES and online_args else ""
    ctx.params["args"] = " ".join(unknown_args) + extra_args
    os.makedirs(ZIPLINE_DIRECTORY, exist_ok=True)

    cloud_provider = get_environ_arg(CLOUD_PROVIDER_KEYWORD, ignoreError=True)

    print(f"Cloud provider: {cloud_provider}")

    if not cloud_provider:
        # Support open source chronon runs
        if chronon_jar:
            Runner(ctx.params, os.path.expanduser(chronon_jar)).run()
        else:
            raise ValueError("Jar path is not set.")
    elif cloud_provider.upper() == GCP:
        ctx.params[ONLINE_JAR_ARG] = ZIPLINE_GCP_JAR_DEFAULT
        ctx.params[ONLINE_CLASS_ARG] = ZIPLINE_GCP_ONLINE_CLASS_DEFAULT
        ctx.params[CLOUD_PROVIDER_KEYWORD] = cloud_provider
        GcpRunner(ctx.params).run()
    elif cloud_provider.upper() == AWS:
        ctx.params[ONLINE_JAR_ARG] = ZIPLINE_AWS_JAR_DEFAULT
        ctx.params[ONLINE_CLASS_ARG] = ZIPLINE_AWS_ONLINE_CLASS_DEFAULT
        ctx.params[CLOUD_PROVIDER_KEYWORD] = cloud_provider
        AwsRunner(ctx.params).run()
    else:
        raise ValueError(f"Unsupported cloud provider: {cloud_provider}")


if __name__ == "__main__":
    main()
