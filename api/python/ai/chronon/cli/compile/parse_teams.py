import importlib
import importlib.util
import os
import sys
from copy import deepcopy
from enum import Enum
from typing import Any, Dict, Optional, Union

from ai.chronon.api.common.ttypes import (
    ConfigProperties,
    EnvironmentVariables,
    ExecutionInfo,
)
from ai.chronon.api.ttypes import Join, MetaData, Team
from ai.chronon.cli.compile.display.console import console
from ai.chronon.cli.logger import get_logger

logger = get_logger()

_DEFAULT_CONF_TEAM = "default"


def import_module_from_file(file_path):
    # Get the module name from the file path (without .py extension)
    module_name = file_path.split("/")[-1].replace(".py", "")

    # Create the module spec
    spec = importlib.util.spec_from_file_location(module_name, file_path)

    # Create the module based on the spec
    module = importlib.util.module_from_spec(spec)

    # Add the module to sys.modules
    sys.modules[module_name] = module

    # Execute the module
    spec.loader.exec_module(module)

    return module


def load_teams(conf_root: str, print: bool = True) -> Dict[str, Team]:

    teams_file = os.path.join(conf_root, "teams.py")

    assert os.path.exists(
        teams_file
    ), f"Team config file: {teams_file} not found. You might be running this from the wrong directory."

    team_module = import_module_from_file(teams_file)

    assert team_module is not None, (
        f"Team config file {teams_file} is not on the PYTHONPATH. You might need to add the your config "
        f"directory to the PYTHONPATH."
    )

    team_dict = {}

    if print:
        console.print(
            f"Pulling configuration from [cyan italic]{teams_file}[/cyan italic]"
        )

    for name, obj in team_module.__dict__.items():
        if isinstance(obj, Team):
            obj.name = name
            team_dict[name] = obj

    return team_dict



def update_metadata(obj: Any, team_dict: Dict[str, Team]):

    assert obj is not None, "Cannot update metadata None object"

    metadata = obj.metaData

    assert obj.metaData is not None, "Cannot update empty metadata"

    name = obj.metaData.name
    team = obj.metaData.team

    assert (
        team is not None
    ), f"Team name is required in metadata for {name}. This usually set by compiler. Internal error."

    assert (
        team in team_dict
    ), f"Team '{team}' not found in teams.py. Please add an entry 🙏"

    assert (
        _DEFAULT_CONF_TEAM in team_dict
    ), f"'{_DEFAULT_CONF_TEAM}' team not found in teams.py, please add an entry 🙏."

    # Only set the outputNamespace if it hasn't been set already
    if not metadata.outputNamespace:
        metadata.outputNamespace = team_dict[team].outputNamespace

    if isinstance(obj, Join):
        join_namespace = obj.metaData.outputNamespace
        # set the metadata for each join part and labelParts
        def set_group_by_metadata(join_part_gb, output_namespace):
            if join_part_gb is not None:
                if join_part_gb.metaData:
                    # Only set the outputNamespace if it hasn't been set already
                    if not join_part_gb.metaData.outputNamespace:
                        join_part_gb.metaData.outputNamespace = output_namespace
                else:
                    # If there's no metaData at all, create it and set outputNamespace
                    join_part_gb.metaData = MetaData()
                    join_part_gb.metaData.outputNamespace = output_namespace

        if obj.joinParts:
            for jp in (obj.joinParts or []):
                set_group_by_metadata(jp.groupBy, join_namespace)

        if obj.labelParts:
            for lb in (obj.labelParts.labels or []):
                set_group_by_metadata(lb.groupBy, join_namespace)

    if metadata.executionInfo is None:
        metadata.executionInfo = ExecutionInfo()

    merge_team_execution_info(metadata, team_dict, team)

def merge_team_execution_info(metadata: MetaData, team_dict: Dict[str, Team], team_name: str):
    default_team = team_dict.get(_DEFAULT_CONF_TEAM)
    if not metadata.executionInfo:
        metadata.executionInfo = ExecutionInfo()

    metadata.executionInfo.env = _merge_mode_maps(
        default_team.env if default_team else {},
        team_dict[team_name].env,
        metadata.executionInfo.env,
        env_or_config_attribute=EnvOrConfigAttribute.ENV,
    )

    metadata.executionInfo.conf = _merge_mode_maps(
        default_team.conf if default_team else {},
        team_dict[team_name].conf,
        metadata.executionInfo.conf,
        env_or_config_attribute=EnvOrConfigAttribute.CONFIG,
    )


def _merge_maps(*maps: Optional[Dict[str, str]]):
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


class EnvOrConfigAttribute(str, Enum):
    ENV = "modeEnvironments"
    CONFIG = "modeConfigs"


def _merge_mode_maps(
    *mode_maps: Union[EnvironmentVariables, ConfigProperties],
    env_or_config_attribute: EnvOrConfigAttribute,
):
    """
    Merges multiple environment variables into one - with the later maps overriding the earlier ones.
    """

    result = None

    final_common = {}

    for mode_map in mode_maps:

        if mode_map is None:
            continue

        if result is None:
            result = deepcopy(mode_map)
            if result.common is not None:
                mode_environments_or_configs = getattr(result, env_or_config_attribute)
                if mode_environments_or_configs:
                    for mode in mode_environments_or_configs:
                        mode_environments_or_configs[mode] = _merge_maps(
                            result.common, mode_environments_or_configs[mode]
                        )

                final_common = _merge_maps(final_common, result.common)
                result.common = None
            continue

        # we don't set common in the env vars, because we want
        # group_by.common to take precedence over team.backfill
        final_common = _merge_maps(final_common, result.common, mode_map.common)

        mode_environments_or_configs = getattr(result, env_or_config_attribute)

        if mode_environments_or_configs:
            for mode in mode_environments_or_configs:
                mode_environments_or_configs[mode] = _merge_maps(
                    mode_environments_or_configs[mode],
                    mode_map.common,
                    getattr(mode_map, env_or_config_attribute).get(mode),
                )

    if result:
        # Want to persist the merged common as the default mode map if
        # user has not explicitly set a mode they want to run, we can use common.
        result.common = final_common

    return result
