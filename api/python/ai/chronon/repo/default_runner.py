import json
import logging
import multiprocessing
import os

from ai.chronon.repo import utils
from ai.chronon.repo.constants import (
    MODE_ARGS,
    ONLINE_CLASS_ARG,
    ONLINE_JAR_ARG,
    ONLINE_MODES,
    ROUTES,
    SPARK_MODES,
    UNIVERSAL_ROUTES,
)


class Runner:
    def __init__(self, args, jar_path):
        self.repo = args["repo"]
        self.conf = args["conf"]
        self.sub_help = args["sub_help"]
        self.mode = args["mode"]
        self.online_jar = args.get(ONLINE_JAR_ARG)
        self.online_class = args.get(ONLINE_CLASS_ARG)

        self.conf_type = args.get("conf_type", "").replace("-", "_")  # in case user sets dash instead of underscore

        # streaming flink
        self.groupby_name = args.get("groupby_name")
        self.kafka_bootstrap = args.get("kafka_bootstrap")
        self.mock_source = args.get("mock_source")
        self.savepoint_uri = args.get("savepoint_uri")
        self.validate = args.get("validate")
        self.validate_rows = args.get("validate_rows")

        valid_jar = args["online_jar"] and os.path.exists(args["online_jar"])

        # fetch online jar if necessary
        if (self.mode in ONLINE_MODES) and (not args["sub_help"]) and not valid_jar and (
                args.get("online_jar_fetch")):
            print("Downloading online_jar")
            self.online_jar = utils.check_output("{}".format(args["online_jar_fetch"])).decode(
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
            utils.check_output("{}".format(self.list_apps_cmd))
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

    def run(self):
        command_list = []
        if self.mode == "info":
            command_list.append(
                "python3 {script} --conf {conf} --ds {ds} --repo {repo}".format(
                    script=self.render_info, conf=self.conf, ds=self.ds, repo=self.repo
                )
            )
        elif self.sub_help or (self.mode not in SPARK_MODES):
            if self.mode == "fetch":
                entrypoint = "ai.chronon.online.fetcher.FetcherMain"
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
                command = self.run_spark_streaming()
                command_list.append(command)
            else:
                if self.parallelism > 1:
                    assert self.start_ds is not None and self.ds is not None, (
                        "To use parallelism, please specify --start-ds and --end-ds to "
                        "break down into multiple backfill jobs"
                    )
                    date_ranges = utils.split_date_range(
                        self.start_ds, self.ds, self.parallelism
                    )
                    for start_ds, end_ds in date_ranges:
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

        if len(command_list) > 1:
            # parallel backfill mode
            with multiprocessing.Pool(processes=int(self.parallelism)) as pool:
                logging.info(
                    "Running args list {} with pool size {}".format(
                        command_list, self.parallelism
                    )
                )
                pool.map(utils.check_call, command_list)
        elif len(command_list) == 1:
            utils.check_call(command_list[0])

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
