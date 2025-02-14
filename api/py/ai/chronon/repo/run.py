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

from google.cloud import storage
import base64
import click
import crcmod
import json
import logging
import multiprocessing
import os
import re
import subprocess
import time
from typing import List
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from enum import Enum

ONLINE_ARGS = "--online-jar={online_jar} --online-class={online_class} "
OFFLINE_ARGS = "--conf-path={conf_path} --end-date={ds} "
ONLINE_WRITE_ARGS = "--conf-path={conf_path} " + ONLINE_ARGS

ONLINE_OFFLINE_WRITE_ARGS = OFFLINE_ARGS + ONLINE_ARGS
ONLINE_MODES = [
    "streaming",
    "metadata-upload",
    "fetch",
    "local-streaming",
    "streaming-client",
]
SPARK_MODES = [
    "backfill",
    "backfill-left",
    "backfill-final",
    "upload",
    "upload-to-kv",
    "streaming",
    "streaming-client",
    "consistency-metrics-compute",
    "compare",
    "analyze",
    "stats-summary",
    "log-summary",
    "log-flattener",
    "metadata-export",
    "label-join",
]
MODES_USING_EMBEDDED = ["metadata-upload", "fetch", "local-streaming"]

# Constants for supporting multiple spark versions.
SUPPORTED_SPARK = ["2.4.0", "3.1.1", "3.2.1", "3.5.1"]
SCALA_VERSION_FOR_SPARK = {"2.4.0": "2.11", "3.1.1": "2.12", "3.2.1": "2.13", "3.5.1": "2.12"}

MODE_ARGS = {
    "backfill": OFFLINE_ARGS,
    "backfill-left": OFFLINE_ARGS,
    "backfill-final": OFFLINE_ARGS,
    "upload": OFFLINE_ARGS,
    "upload-to-kv": ONLINE_WRITE_ARGS,
    "stats-summary": OFFLINE_ARGS,
    "log-summary": OFFLINE_ARGS,
    "analyze": OFFLINE_ARGS,
    "streaming": ONLINE_WRITE_ARGS,
    "metadata-upload": ONLINE_WRITE_ARGS,
    "fetch": ONLINE_ARGS,
    "consistency-metrics-compute": OFFLINE_ARGS,
    "compare": OFFLINE_ARGS,
    "local-streaming": ONLINE_WRITE_ARGS + " -d",
    "log-flattener": OFFLINE_ARGS,
    "metadata-export": OFFLINE_ARGS,
    "label-join": OFFLINE_ARGS,
    "streaming-client": ONLINE_WRITE_ARGS,
    "info": "",
}

ROUTES = {
    "group_bys": {
        "upload": "group-by-upload",
        "upload-to-kv": "groupby-upload-bulk-load",
        "backfill": "group-by-backfill",
        "streaming": "group-by-streaming",
        "metadata-upload": "metadata-upload",
        "local-streaming": "group-by-streaming",
        "fetch": "fetch",
        "analyze": "analyze",
        "metadata-export": "metadata-export",
        "streaming-client": "group-by-streaming",
    },
    "joins": {
        "backfill": "join",
        "backfill-left": "join-left",
        "backfill-final": "join-final",
        "metadata-upload": "metadata-upload",
        "fetch": "fetch",
        "consistency-metrics-compute": "consistency-metrics-compute",
        "compare": "compare-join-query",
        "stats-summary": "stats-summary",
        "log-summary": "log-summary",
        "analyze": "analyze",
        "log-flattener": "log-flattener",
        "metadata-export": "metadata-export",
        "label-join": "label-join",
    },
    "staging_queries": {
        "backfill": "staging-query-backfill",
        "metadata-export": "metadata-export",
    },
}

UNIVERSAL_ROUTES = ["info"]

APP_NAME_TEMPLATE = "chronon_{conf_type}_{mode}_{context}_{name}"
RENDER_INFO_DEFAULT_SCRIPT = "scripts/render_info.py"

# GCP DATAPROC SPECIFIC CONSTANTS
DATAPROC_ENTRY = "ai.chronon.integrations.cloud_gcp.DataprocSubmitter"
ZIPLINE_ONLINE_JAR_DEFAULT = "cloud_gcp_lib_deploy.jar"
ZIPLINE_ONLINE_CLASS_DEFAULT = "ai.chronon.integrations.cloud_gcp.GcpApiImpl"
ZIPLINE_FLINK_JAR_DEFAULT = "flink_assembly_deploy.jar"
ZIPLINE_DATAPROC_SUBMITTER_JAR = "cloud_gcp_submitter_deploy.jar"
ZIPLINE_SERVICE_JAR = "service_assembly_deploy.jar"

ZIPLINE_DIRECTORY = "/tmp/zipline"


class DataprocJobType(Enum):
    SPARK = "spark"
    FLINK = "flink"


def retry_decorator(retries=3, backoff=20):
    def wrapper(func):
        def wrapped(*args, **kwargs):
            attempt = 0
            while attempt <= retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    logging.exception(e)
                    sleep_time = attempt * backoff
                    logging.info(
                        "[{}] Retry: {} out of {}/ Sleeping for {}".format(
                            func.__name__, attempt, retries, sleep_time
                        )
                    )
                    time.sleep(sleep_time)
            return func(*args, **kwargs)

        return wrapped

    return wrapper


def custom_json(conf):
    """Extract the json stored in customJson for a conf."""
    if conf.get("metaData", {}).get("customJson"):
        return json.loads(conf["metaData"]["customJson"])
    return {}


def check_call(cmd):
    print("Running command: " + cmd)
    return subprocess.check_call(cmd.split(), bufsize=0)


def check_output(cmd):
    print("Running command: " + cmd)
    return subprocess.check_output(cmd.split(), bufsize=0).strip()


def download_only_once(url, path, skip_download=False):
    if skip_download:
        print("Skipping download of " + path)
        return
    should_download = True
    path = path.strip()
    if os.path.exists(path):
        content_output = check_output("curl -sI " + url).decode("utf-8")
        content_length = re.search(
            "(content-length:\\s)(\\d+)", content_output.lower())
        remote_size = int(content_length.group().split()[-1])
        local_size = int(check_output("wc -c " + path).split()[0])
        print(
            """Files sizes of {url} vs. {path}
    Remote size: {remote_size}
    Local size : {local_size}""".format(
                **locals()
            )
        )
        if local_size == remote_size:
            print("Sizes match. Assuming it's already downloaded.")
            should_download = False
        if should_download:
            print("Different file from remote at local: " +
                  path + ". Re-downloading..")
            check_call("curl {} -o {} --connect-timeout 10".format(url, path))
    else:
        print("No file at: " + path + ". Downloading..")
        check_call("curl {} -o {} --connect-timeout 10".format(url, path))


# NOTE: this is only for the open source chronon. For the internal zipline version, we have a different jar to download.
@retry_decorator(retries=3, backoff=50)
def download_jar(
        version,
        jar_type="uber",
        release_tag=None,
        spark_version="2.4.0",
        skip_download=False,
):
    assert (spark_version in SUPPORTED_SPARK), (f"Received unsupported spark version {spark_version}. "
                                                f"Supported spark versions are {SUPPORTED_SPARK}")
    scala_version = SCALA_VERSION_FOR_SPARK[spark_version]
    maven_url_prefix = os.environ.get("CHRONON_MAVEN_MIRROR_PREFIX", None)
    default_url_prefix = (
        "https://s01.oss.sonatype.org/service/local/repositories/public/content"
    )
    url_prefix = maven_url_prefix if maven_url_prefix else default_url_prefix
    base_url = "{}/ai/chronon/spark_{}_{}".format(
        url_prefix, jar_type, scala_version)
    print("Downloading jar from url: " + base_url)
    jar_path = os.environ.get("CHRONON_DRIVER_JAR", None)
    if jar_path is None:
        if version == "latest":
            version = None
        if version is None:
            metadata_content = check_output(
                "curl -s {}/maven-metadata.xml".format(base_url)
            )
            meta_tree = ET.fromstring(metadata_content)
            versions = [
                node.text
                for node in meta_tree.findall("./versioning/versions/")
                if re.search(
                    r"^\d+\.\d+\.\d+{}$".format(
                        r"\_{}\d*".format(release_tag) if release_tag else ""
                    ),
                    node.text,
                )
            ]
            version = versions[-1]
        jar_url = "{base_url}/{version}/spark_{jar_type}_{scala_version}-{version}-assembly.jar".format(
            base_url=base_url,
            version=version,
            scala_version=scala_version,
            jar_type=jar_type,
        )
        jar_path = os.path.join("/tmp", extract_filename_from_path(jar_url))
        download_only_once(jar_url, jar_path, skip_download)
    return jar_path


def get_teams_json_file_path(repo_path):
    return os.path.join(repo_path, "teams.json")


def set_runtime_env(params):
    """
    Setting the runtime environment variables.
    These are extracted from the common env, the team env and the common env.
    In order to use the environment variables defined in the configs as overrides for the args in the cli this method
    needs to be run before the runner and jar downloads.

    The order of priority is:
        - Environment variables existing already.
        - Environment variables derived from args (like app_name)
        - conf.metaData.modeToEnvMap for the mode (set on config)
        - team's dev environment for each mode set on teams.json
        - team's prod environment for each mode set on teams.json
        - default team environment per context and mode set on teams.json
        - Common Environment set in teams.json
    """
    environment = {
        "common_env": {},
        "conf_env": {},
        "default_env": {},
        "team_env": {},
        "production_team_env": {},
        "cli_args": {},
    }
    conf_type = None
    # Normalize modes that are effectively replacement of each other (streaming/local-streaming/streaming-client)
    effective_mode = params["mode"]
    if effective_mode and "streaming" in effective_mode:
        effective_mode = "streaming"
    if params["repo"]:
        teams_file = get_teams_json_file_path(params["repo"])
        if os.path.exists(teams_file):
            with open(teams_file, "r") as infile:
                teams_json = json.load(infile)
            # we should have a fallback if user wants to set to something else `default`
            environment["common_env"] = teams_json.get("default", {}).get(
                "common_env", {}
            )
            if params["conf"] and effective_mode:
                try:
                    _, conf_type, team, _ = params["conf"].split("/")[-4:]
                except Exception as e:
                    logging.error(
                        "Invalid conf path: {}, please ensure to supply the relative path to zipline/ folder".format(
                            params["conf"]
                        )
                    )
                    raise e
                if not team:
                    team = "default"
                # context is the environment in which the job is running, which is provided from the args,
                # default to be dev.
                if params["env"]:
                    context = params["env"]
                else:
                    context = "dev"
                logging.info(
                    f"Context: {context} -- conf_type: {conf_type} -- team: {team}"
                )
                conf_path = os.path.join(params["repo"], params["conf"])
                if os.path.isfile(conf_path):
                    with open(conf_path, "r") as conf_file:
                        conf_json = json.load(conf_file)
                    environment["conf_env"] = (
                        conf_json.get("metaData")
                        .get("modeToEnvMap", {})
                        .get(effective_mode, {})
                    )
                    # Load additional args used on backfill.
                    if custom_json(conf_json) and effective_mode in [
                        "backfill",
                        "backfill-left",
                        "backfill-final",
                    ]:
                        environment["conf_env"]["CHRONON_CONFIG_ADDITIONAL_ARGS"] = (
                            " ".join(custom_json(conf_json).get(
                                "additional_args", []))
                        )
                    environment["cli_args"]["APP_NAME"] = APP_NAME_TEMPLATE.format(
                        mode=effective_mode,
                        conf_type=conf_type,
                        context=context,
                        name=conf_json["metaData"]["name"],
                    )
                environment["team_env"] = (
                    teams_json[team].get(context, {}).get(effective_mode, {})
                )
                # fall-back to prod env even in dev mode when dev env is undefined.
                environment["production_team_env"] = (
                    teams_json[team].get("production", {}).get(
                        effective_mode, {})
                )
                # By default use production env.
                environment["default_env"] = (
                    teams_json.get("default", {})
                    .get("production", {})
                    .get(effective_mode, {})
                )
                environment["cli_args"]["CHRONON_CONF_PATH"] = conf_path
    if params["app_name"]:
        environment["cli_args"]["APP_NAME"] = params["app_name"]
    else:
        if not params["app_name"] and not environment["cli_args"].get("APP_NAME"):
            # Provide basic app_name when no conf is defined.
            # Modes like metadata-upload and metadata-export can rely on conf-type or folder rather than a conf.
            environment["cli_args"]["APP_NAME"] = "_".join(
                [
                    k
                    for k in [
                        "chronon",
                        conf_type,
                        params["mode"].replace("-", "_") if params["mode"] else None,
                    ]
                    if k is not None
                ]
            )

    # Adding these to make sure they are printed if provided by the environment.
    environment["cli_args"]["CHRONON_DRIVER_JAR"] = params["chronon_jar"]
    environment["cli_args"]["CHRONON_ONLINE_JAR"] = params["online_jar"]
    environment["cli_args"]["CHRONON_ONLINE_CLASS"] = params["online_class"]
    order = [
        "conf_env",
        "team_env",
        "production_team_env",
        "default_env",
        "common_env",
        "cli_args",
    ]
    print("Setting env variables:")
    for key in os.environ:
        if any([key in environment[set_key] for set_key in order]):
            print(f"From <environment> found {key}={os.environ[key]}")
    for set_key in order:
        for key, value in environment[set_key].items():
            if key not in os.environ and value is not None:
                print(f"From <{set_key}> setting {key}={value}")
                os.environ[key] = value


class Runner:
    def __init__(self, args, jar_path):
        self.repo = args["repo"]
        self.conf = args["conf"]
        self.sub_help = args["sub_help"]
        self.mode = args["mode"]
        self.online_jar = args["online_jar"]
        self.dataproc = args["dataproc"]
        self.conf_type = args.get("conf_type", "").replace("-", "_")  # in case user sets dash instead of underscore

        # streaming flink
        self.groupby_name = args.get("groupby_name")
        self.kafka_bootstrap = args.get("kafka_bootstrap")
        self.mock_source = args.get("mock_source")
        self.savepoint_uri = args.get("savepoint_uri")

        valid_jar = args["online_jar"] and os.path.exists(args["online_jar"])

        # fetch online jar if necessary
        if (self.mode in ONLINE_MODES) and (not args["sub_help"] and not self.dataproc) and not valid_jar and (
                args.get("online_jar_fetch")):
            print("Downloading online_jar")
            self.online_jar = check_output("{}".format(args["online_jar_fetch"])).decode(
                "utf-8"
            )
            os.environ["CHRONON_ONLINE_JAR"] = self.online_jar
            print("Downloaded jar to {}".format(self.online_jar))

        if self.conf:
            try:
                self.context, self.conf_type, self.team, _ = self.conf.split(
                    "/")[-4:]
            except Exception as e:
                logging.error(
                    "Invalid conf path: {}, please ensure to supply the relative path to zipline/ folder".format(
                        self.conf
                    )
                )
                raise e
            possible_modes = list(
                ROUTES[self.conf_type].keys()) + UNIVERSAL_ROUTES
            assert (
                    args["mode"] in possible_modes), ("Invalid mode:{} for conf:{} of type:{}, please choose from {}"
                                                      .format(args["mode"], self.conf, self.conf_type, possible_modes
                                                              ))

        self.ds = args["end_ds"] if "end_ds" in args and args["end_ds"] else args["ds"]
        self.start_ds = (
            args["start_ds"] if "start_ds" in args and args["start_ds"] else None
        )
        self.parallelism = (
            int(args["parallelism"])
            if "parallelism" in args and args["parallelism"]
            else 1
        )
        self.jar_path = jar_path

        self.args = args["args"] if args["args"] else ""
        self.online_class = args["online_class"]
        self.app_name = args["app_name"]
        if self.mode == "streaming":
            self.spark_submit = args["spark_streaming_submit_path"]
        elif self.mode == "info":
            assert os.path.exists(
                args["render_info"]
            ), "Invalid path for the render info script: {}".format(args["render_info"])
            self.render_info = args["render_info"]
        else:
            self.spark_submit = args["spark_submit_path"]
        self.list_apps_cmd = args["list_apps"]

    def run_flink_streaming(self):
        user_args = {
            "--groupby-name": self.groupby_name,
            "--kafka-bootstrap": self.kafka_bootstrap,
            "--online-class": ZIPLINE_ONLINE_CLASS_DEFAULT,
            "-ZGCP_PROJECT_ID": get_gcp_project_id(),
            "-ZGCP_BIGTABLE_INSTANCE_ID": get_gcp_bigtable_instance_id(),
            "--savepoint-uri": self.savepoint_uri
        }

        flag_args = {
            "--mock-source": self.mock_source
        }
        flag_args_str = " ".join(key for key, value in flag_args.items() if value)

        user_args_str = " ".join(f"{key}={value}" for key, value in user_args.items() if value)

        dataproc_args = generate_dataproc_submitter_args(
            job_type=DataprocJobType.FLINK,
            user_args=" ".join([user_args_str, flag_args_str])
        )
        command = f"java -cp {self.jar_path} {DATAPROC_ENTRY} {dataproc_args}"
        return command

    def run_spark_streaming(self):
        # streaming mode
        self.app_name = self.app_name.replace(
            "_streaming-client_", "_streaming_"
        )  # If the job is running cluster mode we want to kill it.
        print(
            "Checking to see if a streaming job by the name {} already exists".format(
                self.app_name
            )
        )
        running_apps = (
            check_output("{}".format(self.list_apps_cmd))
            .decode("utf-8")
            .split("\n")
        )
        running_app_map = {}
        for app in running_apps:
            try:
                app_json = json.loads(app.strip())
                app_name = app_json["app_name"].strip()
                if app_name not in running_app_map:
                    running_app_map[app_name] = []
                running_app_map[app_name].append(app_json)
            except Exception as ex:
                print("failed to process line into app: " + app)
                print(ex)

        filtered_apps = running_app_map.get(self.app_name, [])
        if len(filtered_apps) > 0:
            print(
                "Found running apps by the name {} in \n{}\n".format(
                    self.app_name,
                    "\n".join([str(app) for app in filtered_apps]),
                )
            )
            if self.mode == "streaming":
                assert (len(filtered_apps) == 1), "More than one found, please kill them all"
                print("All good. No need to start a new app.")
                return
            elif self.mode == "streaming-client":
                raise RuntimeError(
                    "Attempting to submit an application in client mode, but there's already"
                    " an existing one running."
                )
        command = (
            "bash {script} --class ai.chronon.spark.Driver {jar} {subcommand} {args} {additional_args}"
        ).format(
            script=self.spark_submit,
            jar=self.jar_path,
            subcommand=ROUTES[self.conf_type][self.mode],
            args=self._gen_final_args(),
            additional_args=os.environ.get(
                "CHRONON_CONFIG_ADDITIONAL_ARGS", ""
            ),
        )
        return command

    def run_streaming(self):
        if self.dataproc:
            return self.run_flink_streaming()
        else:
            return self.run_spark_streaming()

    def run(self):
        command_list = []
        if self.mode == "info":
            command_list.append(
                "python3 {script} --conf {conf} --ds {ds} --repo {repo}".format(
                    script=self.render_info, conf=self.conf, ds=self.ds, repo=self.repo
                )
            )
        elif (self.sub_help or (self.mode not in SPARK_MODES)) and not self.dataproc:
            if self.mode == "fetch":
                entrypoint = "ai.chronon.online.FetcherMain"
            else:
                entrypoint = "ai.chronon.spark.Driver"
            command_list.append(
                "java -cp {jar} {entrypoint} {subcommand} {args}".format(
                    jar=self.jar_path,
                    entrypoint=entrypoint,
                    args="--help" if self.sub_help else self._gen_final_args(),
                    subcommand=ROUTES[self.conf_type][self.mode],
                )
            )
        else:
            if self.mode in ["streaming", "streaming-client"]:
                # streaming mode
                command = self.run_streaming()
                command_list.append(command)
            else:

                if self.parallelism > 1:
                    assert self.start_ds is not None and self.ds is not None, (
                        "To use parallelism, please specify --start-ds and --end-ds to "
                        "break down into multiple backfill jobs"
                    )
                    date_ranges = split_date_range(
                        self.start_ds, self.ds, self.parallelism
                    )
                    for start_ds, end_ds in date_ranges:
                        if not self.dataproc:
                            command = (
                                    "bash {script} --class ai.chronon.spark.Driver " +
                                    "{jar} {subcommand} {args} {additional_args}"
                            ).format(
                                script=self.spark_submit,
                                jar=self.jar_path,
                                subcommand=ROUTES[self.conf_type][self.mode],
                                args=self._gen_final_args(
                                    start_ds=start_ds, end_ds=end_ds),
                                additional_args=os.environ.get(
                                    "CHRONON_CONFIG_ADDITIONAL_ARGS", ""
                                ),
                            )
                            command_list.append(command)
                        else:
                            user_args = (
                                "{subcommand} {args} {additional_args}"
                            ).format(
                                subcommand=ROUTES[self.conf_type][self.mode],
                                args=self._gen_final_args(start_ds=self.start_ds,
                                                          end_ds=end_ds,
                                                          # overriding the conf here because we only want the
                                                          # filename, not the full path. When we upload this to
                                                          # GCS, the full path does get reflected on GCS. But
                                                          # when we include the gcs file path as part of dataproc,
                                                          # the file is copied to root and not the complete path
                                                          # is copied.
                                                          override_conf_path=extract_filename_from_path(
                                                              self.conf) if self.conf else None),
                                additional_args=os.environ.get(
                                    "CHRONON_CONFIG_ADDITIONAL_ARGS", ""
                                ),
                            )
                            local_files_to_upload_to_gcs = []
                            if self.conf:
                                local_files_to_upload_to_gcs.append(
                                    self.conf)

                            dataproc_args = generate_dataproc_submitter_args(
                                local_files_to_upload_to_gcs=[self.conf],
                                # for now, self.conf is the only local file that requires uploading to gcs
                                user_args=user_args
                            )
                            command = f"java -cp {self.jar_path} {DATAPROC_ENTRY} {dataproc_args}"
                        command_list.append(command)
                else:
                    if not self.dataproc:
                        command = (
                                "bash {script} --class ai.chronon.spark.Driver "
                                + "{jar} {subcommand} {args} {additional_args}"
                        ).format(
                            script=self.spark_submit,
                            jar=self.jar_path,
                            subcommand=ROUTES[self.conf_type][self.mode],
                            args=self._gen_final_args(self.start_ds),
                            additional_args=os.environ.get(
                                "CHRONON_CONFIG_ADDITIONAL_ARGS", ""
                            ),
                        )
                        command_list.append(command)
                    else:
                        user_args = (
                            "{subcommand} {args} {additional_args}"
                        ).format(
                            subcommand=ROUTES[self.conf_type][self.mode],
                            args=self._gen_final_args(start_ds=self.start_ds,
                                                      # overriding the conf here because we only want the filename,
                                                      # not the full path. When we upload this to GCS, the full path
                                                      # does get reflected on GCS. But when we include the gcs file
                                                      # path as part of dataproc, the file is copied to root and
                                                      # not the complete path is copied.
                                                      override_conf_path=extract_filename_from_path(
                                                          self.conf) if self.conf else None),
                            additional_args=os.environ.get(
                                "CHRONON_CONFIG_ADDITIONAL_ARGS", ""
                            ),
                        )
                        local_files_to_upload_to_gcs = []
                        if self.conf:
                            local_files_to_upload_to_gcs.append(self.conf)

                        dataproc_args = generate_dataproc_submitter_args(
                            # for now, self.conf is the only local file that requires uploading to gcs
                            local_files_to_upload_to_gcs=local_files_to_upload_to_gcs,
                            user_args=user_args
                        )
                        command = f"java -cp {self.jar_path} {DATAPROC_ENTRY} {dataproc_args}"
                    command_list.append(command)

        if len(command_list) > 1:
            # parallel backfill mode
            with multiprocessing.Pool(processes=int(self.parallelism)) as pool:
                logging.info(
                    "Running args list {} with pool size {}".format(
                        command_list, self.parallelism
                    )
                )
                pool.map(check_call, command_list)
        elif len(command_list) == 1:
            if self.dataproc:
                output = check_output(command_list[0]).decode("utf-8").split("\n")
                print(*output, sep="\n")

                dataproc_submitter_id_str = "Dataproc submitter job id"

                dataproc_submitter_logs = [s for s in output if dataproc_submitter_id_str in s]
                if dataproc_submitter_logs:
                    log = dataproc_submitter_logs[0]
                    job_id = log[log.index(dataproc_submitter_id_str) + len(dataproc_submitter_id_str) + 1:]
                    try:
                        print("""
                        <-----------------------------------------------------------------------------------
                        ------------------------------------------------------------------------------------
                                                          DATAPROC LOGS
                        ------------------------------------------------------------------------------------
                        ------------------------------------------------------------------------------------>
                        """)
                        check_call(f"gcloud dataproc jobs wait {job_id} --region={get_gcp_region_id()}")
                    except Exception:
                        # swallow since this is just for tailing logs
                        pass
            else:
                check_call(command_list[0])

    def _gen_final_args(self, start_ds=None, end_ds=None, override_conf_path=None, **kwargs):
        base_args = MODE_ARGS[self.mode].format(
            conf_path=override_conf_path if override_conf_path else self.conf,
            ds=end_ds if end_ds else self.ds,
            online_jar=self.online_jar,
            online_class=self.online_class
        )
        base_args = base_args + f" --conf-type={self.conf_type} " if self.conf_type else base_args

        override_start_partition_arg = (
            "--start-partition-override=" + start_ds if start_ds else ""
        )

        additional_args = " ".join(f"--{key.replace('_', '-')}={value}" for key, value in kwargs.items() if value)

        final_args = " ".join([base_args, str(self.args), override_start_partition_arg, additional_args])

        return final_args


def extract_filename_from_path(path):
    return path.split("/")[-1]


def split_date_range(start_date, end_date, parallelism):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    if start_date > end_date:
        raise ValueError("Start date should be earlier than end date")
    total_days = (
                         end_date - start_date
                 ).days + 1  # +1 to include the end_date in the range

    # Check if parallelism is greater than total_days
    if parallelism > total_days:
        raise ValueError(
            "Parallelism should be less than or equal to total days")

    split_size = total_days // parallelism
    date_ranges = []

    for i in range(parallelism):
        split_start = start_date + timedelta(days=i * split_size)
        if i == parallelism - 1:
            split_end = end_date
        else:
            split_end = split_start + timedelta(days=split_size - 1)
        date_ranges.append(
            (split_start.strftime("%Y-%m-%d"), split_end.strftime("%Y-%m-%d"))
        )
    return date_ranges


def set_defaults(ctx):
    """Set default values based on environment"""
    chronon_repo_path = os.environ.get("CHRONON_REPO_PATH", ".")
    today = datetime.today().strftime("%Y-%m-%d")
    defaults = {
        "mode": "backfill",
        "dataproc": False,
        "ds": today,
        "app_name": os.environ.get("APP_NAME"),
        "online_jar": os.environ.get("CHRONON_ONLINE_JAR"),
        "repo": chronon_repo_path,
        "online_class": os.environ.get("CHRONON_ONLINE_CLASS"),
        "version": os.environ.get("VERSION"),
        "spark_version": os.environ.get("SPARK_VERSION", "2.4.0"),
        "spark_submit_path": os.path.join(chronon_repo_path, "scripts/spark_submit.sh"),
        "spark_streaming_submit_path": os.path.join(
            chronon_repo_path, "scripts/spark_streaming.sh"
        ),
        # NOTE: We don't want to ever call the fetch_online_jar.py script since we're working
        # on our internal zipline fork of the chronon repo
        # "online_jar_fetch": os.path.join(chronon_repo_path, "scripts/fetch_online_jar.py"),
        "conf_type": "group_bys",
        "online_args": os.environ.get("CHRONON_ONLINE_ARGS", ""),
        "chronon_jar": os.environ.get("CHRONON_DRIVER_JAR"),
        "list_apps": "python3 " + os.path.join(chronon_repo_path, "scripts/yarn_list.py"),
        "render_info": os.path.join(chronon_repo_path, RENDER_INFO_DEFAULT_SCRIPT),
    }
    for key, value in defaults.items():
        if ctx.params.get(key) is None and value is not None:
            ctx.params[key] = value


def get_environ_arg(env_name) -> str:
    value = os.environ.get(env_name)
    if not value:
        raise ValueError(f"Please set {env_name} environment variable")
    return value


def get_customer_id() -> str:
    return get_environ_arg('CUSTOMER_ID')


def get_gcp_project_id() -> str:
    return get_environ_arg('GCP_PROJECT_ID')


def get_gcp_bigtable_instance_id() -> str:
    return get_environ_arg('GCP_BIGTABLE_INSTANCE_ID')


def get_gcp_region_id() -> str:
    return get_environ_arg('GCP_REGION')


def generate_dataproc_submitter_args(user_args: str, job_type: DataprocJobType = DataprocJobType.SPARK,
                                     local_files_to_upload_to_gcs: List[str] = []):
    customer_warehouse_bucket_name = f"zipline-warehouse-{get_customer_id()}"

    gcs_files = []
    for source_file in local_files_to_upload_to_gcs:
        # upload to `metadata` folder
        destination_file_path = f"metadata/{extract_filename_from_path(source_file)}"
        gcs_files.append(upload_gcs_blob(
            customer_warehouse_bucket_name, source_file, destination_file_path))

    # we also want the additional-confs included here. it should already be in the bucket

    zipline_artifacts_bucket_prefix = 'gs://zipline-artifacts'

    gcs_files.append(
        f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}/confs/additional-confs.yaml")

    gcs_file_args = ",".join(gcs_files)

    # include jar uri. should also already be in the bucket
    jar_uri = f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}" + \
              f"/jars/{ZIPLINE_ONLINE_JAR_DEFAULT}"

    final_args = "{user_args} --jar-uri={jar_uri} --job-type={job_type} --main-class={main_class}"

    if job_type == DataprocJobType.FLINK:
        main_class = "ai.chronon.flink.FlinkJob"
        flink_jar_uri = f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}" + f"/jars/{ZIPLINE_FLINK_JAR_DEFAULT}"
        return final_args.format(
            user_args=user_args,
            jar_uri=jar_uri,
            job_type=job_type.value,
            main_class=main_class
        ) + f" --flink-main-jar-uri={flink_jar_uri}"

    elif job_type == DataprocJobType.SPARK:
        main_class = "ai.chronon.spark.Driver"
        return final_args.format(
            user_args=user_args,
            jar_uri=jar_uri,
            job_type=job_type.value,
            main_class=main_class
        ) + f" --additional-conf-path=additional-confs.yaml --gcs-files={gcs_file_args}"
    else:
        raise ValueError(f"Invalid job type: {job_type}")


def download_zipline_jar(destination_dir: str, customer_id: str, jar_name: str):
    bucket_name = f"zipline-artifacts-{customer_id}"

    source_blob_name = f"jars/{jar_name}"
    destination_path = f"{destination_dir}/{jar_name}"

    are_identical = compare_gcs_and_local_file_hashes(bucket_name, source_blob_name,
                                                      destination_path) if os.path.exists(
        destination_path) else False

    if are_identical:
        print(
            f"{destination_path} matches GCS {bucket_name}/{source_blob_name}")
    else:
        print(
            f"{destination_path} does NOT match GCS {bucket_name}/{source_blob_name}")
        print(f"Downloading {jar_name} from GCS...")

        download_gcs_blob(bucket_name, source_blob_name,
                          destination_path)
    return destination_path


@retry_decorator(retries=2, backoff=5)
def download_gcs_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    try:
        storage_client = storage.Client(project=get_gcp_project_id())
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(
            "Downloaded storage object {} from bucket {} to local file {}.".format(
                source_blob_name, bucket_name, destination_file_name
            )
        )
    except Exception as e:
        raise RuntimeError(f"Failed to download {source_blob_name}: {str(e)}")


@retry_decorator(retries=2, backoff=5)
def upload_gcs_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    try:
        storage_client = storage.Client(project=get_gcp_project_id())
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        print(
            f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}."
        )
        return f"gs://{bucket_name}/{destination_blob_name}"
    except Exception as e:
        raise RuntimeError(f"Failed to upload {source_file_name}: {str(e)}")


def get_gcs_file_hash(bucket_name: str, blob_name: str) -> str:
    """
    Get the hash of a file stored in Google Cloud Storage.
    """
    storage_client = storage.Client(project=get_gcp_project_id())
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    if not blob:
        raise FileNotFoundError(f"File {blob_name} not found in bucket {bucket_name}")

    return blob.crc32c


def get_local_file_hash(file_path: str) -> str:
    """
    Calculate CRC32C hash of a local file.

    Args:
        file_path: Path to the local file

    Returns:
        Base64-encoded string of the file's CRC32C hash
    """
    crc32c_hash = crcmod.predefined.Crc('crc-32c')

    with open(file_path, "rb") as f:
        # Read the file in chunks to handle large files efficiently
        for chunk in iter(lambda: f.read(4096), b""):
            crc32c_hash.update(chunk)

    # Convert to base64 to match GCS format
    return base64.b64encode(crc32c_hash.digest()).decode('utf-8')


def compare_gcs_and_local_file_hashes(bucket_name: str, blob_name: str, local_file_path: str) -> bool:
    """
    Compare hashes of a GCS file and a local file to check if they're identical.

    Args:
        bucket_name: Name of the GCS bucket
        blob_name: Name/path of the blob in the bucket
        local_file_path: Path to the local file

    Returns:
        True if files are identical, False otherwise
    """
    try:
        gcs_hash = get_gcs_file_hash(bucket_name, blob_name)
        local_hash = get_local_file_hash(local_file_path)

        print(f"Local hash of {local_file_path}: {local_hash}. GCS file {blob_name} hash: {gcs_hash}")

        return gcs_hash == local_hash

    except Exception as e:
        print(f"Error comparing files: {str(e)}")
        return False


@click.command(name="run", context_settings=dict(allow_extra_args=True, ignore_unknown_options=True))
@click.option("--conf", required=False, help="Conf param - required for every mode except fetch")
@click.option("--env", required=False, default="dev", help="Running environment - default to be dev")
@click.option("--mode", type=click.Choice(MODE_ARGS.keys()))
@click.option("--dataproc", is_flag=True, help="Run on dataproc")
@click.option("--ds", help="the end partition to backfill the data")
@click.option("--app-name", help="app name. Default to {}".format(APP_NAME_TEMPLATE))
@click.option("--start-ds", help="override the original start partition for a range backfill. "
                                 "It only supports staging query, group by backfill and join jobs. "
                                 "It could leave holes in your final output table due to the override date range.")
@click.option("--end-ds", help="the end ds for a range backfill")
@click.option("--parallelism", help="break down the backfill range into this number of tasks in parallel. "
                                    "Please use it along with --start-ds and --end-ds and only in manual mode")
@click.option("--repo", help="Path to chronon repo", default=".")
@click.option("--online-jar", default=ZIPLINE_ONLINE_JAR_DEFAULT,
              help="Jar containing Online KvStore & Deserializer Impl. "
                   "Used for streaming and metadata-upload mode.")
@click.option("--online-class", default=ZIPLINE_ONLINE_CLASS_DEFAULT,
              help="Class name of Online Impl. Used for streaming and metadata-upload mode.")
@click.option("--version", help="Chronon version to use.")
@click.option("--spark-version", default="2.4.0", help="Spark version to use for downloading jar.")
@click.option("--spark-submit-path", help="Path to spark-submit")
@click.option("--spark-streaming-submit-path", help="Path to spark-submit for streaming")
@click.option("--online-jar-fetch", help="Path to script that can pull online jar. This will run only "
                                         "when a file doesn't exist at location specified by online_jar")
@click.option("--sub-help", is_flag=True, help="print help command of the underlying jar and exit")
@click.option("--conf-type", help="related to sub-help - no need to set unless you are not working with a conf")
@click.option("--online-args", help="Basic arguments that need to be supplied to all online modes")
@click.option("--chronon-jar", help="Path to chronon OS jar")
@click.option("--release-tag", help="Use the latest jar for a particular tag.")
@click.option("--list-apps", help="command/script to list running jobs on the scheduler")
@click.option("--render-info", help="Path to script rendering additional information of the given config. "
                                    "Only applicable when mode is set to info")
@click.option("--groupby-name", help="Name of groupby to be used for groupby streaming")
@click.option("--kafka-bootstrap", help="Kafka bootstrap server in host:port format")
@click.option("--mock-source", is_flag=True,
              help="Use a mocked data source instead of a real source for groupby-streaming Flink.")
@click.option("--savepoint-uri", help="Savepoint URI for Flink streaming job")
@click.pass_context
def main(ctx, conf, env, mode, dataproc, ds, app_name, start_ds, end_ds, parallelism, repo, online_jar, online_class,
         version, spark_version, spark_submit_path, spark_streaming_submit_path, online_jar_fetch, sub_help, conf_type,
         online_args, chronon_jar, release_tag, list_apps, render_info, groupby_name, kafka_bootstrap, mock_source,
         savepoint_uri):
    unknown_args = ctx.args
    click.echo("Running with args: {}".format(ctx.params))
    set_runtime_env(ctx.params)
    set_defaults(ctx)
    extra_args = (" " + online_args) if mode in ONLINE_MODES and online_args else ""
    ctx.params["args"] = " ".join(unknown_args) + extra_args
    os.makedirs(ZIPLINE_DIRECTORY, exist_ok=True)

    if dataproc:
        jar_path = download_zipline_jar(ZIPLINE_DIRECTORY, get_customer_id(), ZIPLINE_DATAPROC_SUBMITTER_JAR)
    elif chronon_jar:
        jar_path = chronon_jar
    else:
        service_jar_path = download_zipline_jar(ZIPLINE_DIRECTORY, get_customer_id(), ZIPLINE_SERVICE_JAR)
        chronon_gcp_jar_path = download_zipline_jar(ZIPLINE_DIRECTORY, get_customer_id(), ZIPLINE_ONLINE_JAR_DEFAULT)
        jar_path = f"{service_jar_path}:{chronon_gcp_jar_path}"

    Runner(ctx.params, os.path.expanduser(jar_path)).run()


if __name__ == "__main__":
    main()
