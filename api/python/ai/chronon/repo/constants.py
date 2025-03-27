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

ZIPLINE_DIRECTORY = "/tmp/zipline"

CLOUD_PROVIDER_KEYWORD = 'CLOUD_PROVIDER'

# cloud provider
AWS = 'AWS'
GCP = 'GCP'

# arg keywords
ONLINE_CLASS_ARG = 'online_class'
ONLINE_JAR_ARG = 'online_jar'
