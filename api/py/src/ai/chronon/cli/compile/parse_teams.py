import importlib
import os
from gen_thrift.api.ttypes import Team, EnvironmentVariables
from ai.chronon.cli.logger import get_logger, require
from typing import Any, List, Dict

logger = get_logger()

_DEFAULT_CONF_TEAM = "common"


def load_teams(conf_root: str) -> Dict[str, Team]:

    teams_file = os.path.join(conf_root, "teams.py")

    require(
        os.path.exists(teams_file),
        f"Team config file: {teams_file} not found. Please add one.",
    )

    team_module = importlib.import_module("teams")

    require(
        team_module is not None,
        f"Team config file {teams_file} is not on the PYTHONPATH",
    )

    team_dict = {}

    for name, obj in team_module.__dict__.items():
        if isinstance(obj, Team):
            obj.name = name
            logger.info(f"Found team {name}")
            team_dict[name] = obj

    return team_dict


def update_metadata(obj: Any, team_dict: Dict[str, Team]):

    require(obj is not None, "Cannot update metadata None object")

    metadata = obj.metaData

    require(obj.metaData is not None, "Cannot update empty metadata")

    name = obj.metaData.name
    team = obj.metaData.team

    require(
        team is not None,
        f"Team name is required in metadata for {name}. This usually set by compiler. Internal error.",
    )

    require(
        team in team_dict, f"Team '{team}' not found in teams.py. Please add an entry."
    )

    require(
        _DEFAULT_CONF_TEAM in team_dict,
        f"'{_DEFAULT_CONF_TEAM}' team not found in teams.py, please add an entry 🙏",
    )

    metadata.outputNamespace = team_dict[team].outputNamespace

    metadata.env = _merge_environment_variables(
        team_dict[_DEFAULT_CONF_TEAM].env,
        team_dict[team].env,
        metadata.env,
    )


def _merge_maps(*maps: List[Dict[str, str]]):
    """
    Merges multiple maps into one - with the later maps overriding the earlier ones.
    """

    result = {}

    for m in maps:

        if m is None:
            continue

        for key, value in m.items():
            result[key] = value

    return result


def _merge_environment_variables(*env_vars: List[EnvironmentVariables]):
    """
    Merges multiple environment variables into one - with the later maps overriding the earlier ones.
    """

    result = EnvironmentVariables()

    for env_var in env_vars:

        if env_var is None:
            continue

        result.backfill = _merge_maps(result.backfill, env_var.common, env_var.backfill)
        result.upload = _merge_maps(result.upload, env_var.common, env_var.upload)
        result.streaming = _merge_maps(
            result.streaming, env_var.common, env_var.streaming
        )

    return result
