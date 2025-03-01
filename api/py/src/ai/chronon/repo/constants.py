from gen_thrift.api.ttypes import GroupBy, Join, StagingQuery, Model

JOIN_FOLDER_NAME = "joins"
GROUP_BY_FOLDER_NAME = "group_bys"
STAGING_QUERY_FOLDER_NAME = "staging_queries"
MODEL_FOLDER_NAME = "models"
# TODO - make team part of thrift API?
TEAMS_FILE_PATH = "teams.json"
OUTPUT_ROOT = "production"

# This is set in the main function -
# from command line or from env variable during invocation
FOLDER_NAME_TO_CLASS = {
    GROUP_BY_FOLDER_NAME: GroupBy,
    JOIN_FOLDER_NAME: Join,
    STAGING_QUERY_FOLDER_NAME: StagingQuery,
    MODEL_FOLDER_NAME: Model,
}
