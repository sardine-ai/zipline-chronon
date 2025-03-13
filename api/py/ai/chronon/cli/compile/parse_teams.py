from copy import deepcopy
import importlib
import os
from ai.chronon.api.common.ttypes import (
    ExecutionInfo,
    ConfigProperties,
    EnvironmentVariables,
)
from ai.chronon.api.ttypes import Team
from ai.chronon.cli.logger import get_logger, require
from typing import Any, List, Dict, Union

logger = get_logger()

_DEFAULT_CONF_TEAM = "common"


def load_teams(conf_root: str) -> Dict[str, Team]:

    teams_file = os.path.join(conf_root, "teams.py")

    require(
        os.path.exists(teams_file),
        f"Team config file: {teams_file} not found. You might be running this from the wrong directory.",
    )

    team_module = importlib.import_module("teams")

    require(
        team_module is not None,
        f"Team config file {teams_file} is not on the PYTHONPATH. You might need to add the your config directory to the PYTHONPATH.",
    )

    team_dict = {}

    for name, obj in team_module.__dict__.items():
        if isinstance(obj, Team):
            obj.name = name
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
        team in team_dict,
        f"Team '{team}' not found in teams.py. Please add an entry 🙏",
    )

    require(
        _DEFAULT_CONF_TEAM in team_dict,
        f"'{_DEFAULT_CONF_TEAM}' team not found in teams.py, please add an entry 🙏",
    )

    metadata.outputNamespace = team_dict[team].outputNamespace

    if metadata.executionInfo is None:
        metadata.executionInfo = ExecutionInfo()

    metadata.executionInfo.env = _merge_mode_maps(
        team_dict[_DEFAULT_CONF_TEAM].env,
        team_dict[team].env,
        metadata.executionInfo.env,
    )

    metadata.executionInfo.conf = _merge_mode_maps(
        team_dict[_DEFAULT_CONF_TEAM].conf,
        team_dict[team].conf,
        metadata.executionInfo.conf,
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


def _merge_mode_maps(
    *mode_maps: Union[List[EnvironmentVariables], List[ConfigProperties]]
):
    """
    Merges multiple environment variables into one - with the later maps overriding the earlier ones.
    """

    result = None

    for mode_map in mode_maps:

        if mode_map is None:
            continue

        if result is None:
            result = deepcopy(mode_map)
            if result.common is not None:
                result.backfill = _merge_maps(result.common, result.backfill)
                result.upload = _merge_maps(result.common, result.upload)
                result.streaming = _merge_maps(result.common, result.streaming)
                result.serving = _merge_maps(result.common, result.serving)
                result.common = None
            continue

        # we don't set common in the env vars, because we want
        # group_by.common to take precedence over team.backfill
        result.backfill = _merge_maps(
            result.backfill, mode_map.common, mode_map.backfill
        )
        result.upload = _merge_maps(result.upload, mode_map.common, mode_map.upload)
        result.streaming = _merge_maps(
            result.streaming, mode_map.common, mode_map.streaming
        )
        result.serving = _merge_maps(result.serving, mode_map.common, mode_map.serving)

    return result
