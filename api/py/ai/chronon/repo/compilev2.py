#!/usr/bin/env python
# tool to compile StagingQueries, GroupBys and Joins into thrift configurations
# that chronon jobs can consume


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

import logging
import os

import ai.chronon.repo.extract_objects as eo
import ai.chronon.utils as utils
from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.repo import (
    FOLDER_NAME_TO_CLASS,
    TEAMS_FILE_PATH,
)
from ai.chronon.repo import teams
from ai.chronon.repo.serializer import thrift_simple_json_protected
from ai.chronon.repo.validator import (
    ChrononRepoValidator,
    get_join_output_columns,
    get_group_by_output_columns,
)


DEFAULT_TEAM_NAME = "default"


def get_folder_name_from_class_name(class_name):
    return {v.__name__: k for k, v in FOLDER_NAME_TO_CLASS.items()}[class_name]


def extract_and_convert(
    chronon_root, target_object, target_object_file, debug=False, output_root=None
):
    """
    Compiles the entire Chronon repository, however it treats `target_object` in a special manner. If compilation
    of this object fails, then this exits with the failure message. Else it shows failing compilations as a warning
    and proceeds.

    It also logs lineage and output schema for target_object (TODO -- ZiplineHub integration).
    """
    if debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    if not output_root:
        output_root = chronon_root

    _print_highlighted("Using chronon root path", chronon_root)

    chronon_root_path = os.path.expanduser(chronon_root)
    utils.chronon_root_path = chronon_root_path

    # Get list of subdirectories in input_path that match FOLDER_NAME_TO_CLASS keys
    obj_folder_names = [
        d for d in os.listdir(chronon_root) if d in FOLDER_NAME_TO_CLASS.keys()
    ]
    assert (
        obj_folder_names
    ), f"No valid chronon subdirs {FOLDER_NAME_TO_CLASS.keys()} found within {chronon_root}"
    _print_highlighted(
        f"Compiling the following directories within {chronon_root_path}:",
        f"\n {obj_folder_names} ",
    )

    validator = ChrononRepoValidator(
        chronon_root, os.path.join(chronon_root_path, "production"), log_level=log_level
    )

    compile_errors = {}

    for obj_folder_name in obj_folder_names:

        obj_class = FOLDER_NAME_TO_CLASS[obj_folder_name]
        object_input_path = os.path.join(chronon_root_path, obj_folder_name)

        results, obj_folder_errors, target_file_error = eo.from_folderV2(
            object_input_path, target_object_file, obj_class
        )

        if target_file_error:
            raise ValueError(
                f"Error in file {target_object_file}: \n {target_file_error}"
            )

        compile_errors.update(obj_folder_errors)

        full_output_root = os.path.join(chronon_root_path, output_root)
        teams_path = os.path.join(chronon_root_path, TEAMS_FILE_PATH)

        for name, (obj, origin_file) in results.items():
            team_name = name.split(".")[0]
            _set_team_level_metadata(obj, teams_path, team_name)
            _set_templated_values(obj, obj_class, teams_path, team_name)
            obj_write_errors = _write_obj(full_output_root, validator, name, obj)
            if obj_write_errors:
                compile_errors[origin_file] = obj_write_errors
            else:
                # In case of online join, we need to make sure that upstream GBs are online
                if obj_class is Join and obj.metaData.online:
                    offline_gbs = [
                        jp for jp in obj.joinParts if not jp.groupBy.metaData.online
                    ]
                    assert not offline_gbs, (
                        "You must make all dependent GroupBys `online` if you want to make your join `online`."
                        " You can do this by passing the `online=True` argument to the GroupBy constructor."
                        " Fix the following: {}".format(offline_gbs)
                    )

    if compile_errors:
        create_error_logs(compile_errors, chronon_root)

    show_lineage_and_schema(target_object)


def show_lineage_and_schema(target_object):
    """
    Shows useful information to the user about their compiled object
    """

    try:
        """
        Talk to ZiplineHub and get back somemething to display.

        Most important things to show here:
          - Lineage (esp upstream, but downstream as well)
          - Output schema for target objecct
        Open questions:
          - Is it a link to UI, or HTML to render in-cell (if notebook), and/or ascii if CLI?
        """
        raise NotImplementedError("TODO")
    except Exception as e:

        _print_warning(
            f"Failed to connect to ZiplineHub: {str(e)}\n\n"
            + "Showing output column names, but cannot show schema/lineage without ZiplineHub.\n\n"
        )

        obj_class = target_object.__class__

        if obj_class is Join:
            _print_features_names(
                f"Output Features for {target_object.metaData.name} (Join):",
                "\n - " + "\n - ".join(get_join_output_columns(target_object)),
            )

        if obj_class is GroupBy:
            _print_features_names(
                f"Output GroupBy Features for {target_object.metaData.name} (GroupBy)",
                "\n - " + "\n - ".join(get_group_by_output_columns(target_object)),
            )


def create_error_logs(compile_errors, chronon_root_path: str):
    """
    Creates an error log file containing compilation errors for each file.

    Args:
        errors: Dict mapping filenames to exception strings
        chronon_root_path: Path to chronon root directory
    """

    error_log_path = os.path.join(chronon_root_path, "errors.log")

    with open(error_log_path, "w") as f:
        f.write("Compilation errors: \n\n")
        for filename, error in compile_errors.items():
            f.write(f"{filename}\n\n")
            f.write(f"{str(error)}\n\n")

    _print_warning(
        "\n\n Warning -- The following files have errors preventing the compilation of Zipline objects:\n\n"
        + "\n".join(
            [
                f"- {os.path.relpath(file, chronon_root_path)}"
                for file in compile_errors.keys()
            ]
        )
        + f"\n\n\nSee {error_log_path} for more details.\n"
    )


def _set_team_level_metadata(obj: object, teams_path: str, team_name: str):
    namespace = teams.get_team_conf(teams_path, team_name, "namespace")
    table_properties = teams.get_team_conf(teams_path, team_name, "table_properties")
    obj.metaData.outputNamespace = obj.metaData.outputNamespace or namespace
    obj.metaData.tableProperties = obj.metaData.tableProperties or table_properties
    obj.metaData.team = team_name

    # set metadata for JoinSource
    if isinstance(obj, GroupBy):
        for source in obj.sources:
            if source.joinSource:
                _set_team_level_metadata(source.joinSource.join, teams_path, team_name)


def __fill_template(table, obj, namespace):
    if table:
        table = table.replace(
            "{{ logged_table }}", utils.log_table_name(obj, full_name=True)
        )
        table = table.replace("{{ db }}", namespace)
    return table


def _set_templated_values(obj, cls, teams_path, team_name):
    namespace = teams.get_team_conf(teams_path, team_name, "namespace")
    if cls == Join and obj.bootstrapParts:
        for bootstrap in obj.bootstrapParts:
            bootstrap.table = __fill_template(bootstrap.table, obj, namespace)
        if obj.metaData.dependencies:
            obj.metaData.dependencies = [
                __fill_template(dep, obj, namespace)
                for dep in obj.metaData.dependencies
            ]
    if cls == Join and obj.labelParts:
        obj.labelParts.metaData.dependencies = [
            label_dep.replace(
                "{{ join_backfill_table }}",
                utils.output_table_name(obj, full_name=True),
            )
            for label_dep in obj.labelParts.metaData.dependencies
        ]


def _write_obj(
    full_output_root: str, validator: ChrononRepoValidator, name: str, obj: object
) -> str:
    """
    Returns errors if failed to write, else None for success
    """
    team_name = name.split(".")[0]
    obj_class = type(obj)
    class_name = obj_class.__name__
    name = name.split(".", 1)[1]
    obj_folder_name = get_folder_name_from_class_name(class_name)
    output_path = os.path.join(full_output_root, obj_folder_name, team_name)
    output_file = os.path.join(output_path, name)
    validation_errors = validator.validate_obj(obj)
    if validation_errors:
        return ", ".join(validation_errors)
    # elif not validator.safe_to_overwrite(obj):
    #    return f"Cannot overwrite {class_name} {name} with existing online conf"
    _write_obj_as_json(name, obj, output_file, obj_class)
    return None


def _write_obj_as_json(name: str, obj: object, output_file: str, obj_class: type):
    output_folder = os.path.dirname(output_file)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    assert os.path.isdir(output_folder), f"{output_folder} isn't a folder."
    assert hasattr(obj, "name") or hasattr(
        obj, "metaData"
    ), f"Can't serialize objects without the name attribute for object {name}"
    with open(output_file, "w") as f:
        f.write(thrift_simple_json_protected(obj, obj_class))


def _print_highlighted(left, right):
    # print in blue.
    print(f"{left:>25} - \u001b[34m{right}\u001b[0m")


def _print_features_names(left, right):
    # Print in green and separate lines.
    print(f"{left:>25} \u001b[32m{right}\u001b[0m")


def _print_error(left, right):
    # print in red.
    print(f"\033[91m{left:>25} \033[1m{right}\033[00m")


def _print_warning(string):
    # print in yellow - \u001b[33m
    print(f"\u001b[33m{string}\u001b[0m")
