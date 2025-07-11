import copy
import glob
import importlib
import os
from typing import List

from ai.chronon import airflow_helpers
from ai.chronon.cli.compile import parse_teams, serializer
from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj
from ai.chronon.cli.logger import get_logger

logger = get_logger()

def from_folder(
    cls: type, input_dir: str, compile_context: CompileContext
) -> List[CompiledObj]:
    """
    Recursively consumes a folder, and constructs a map of
    object qualifier to StagingQuery, GroupBy, or Join
    """

    python_files = glob.glob(os.path.join(input_dir, "**/*.py"), recursive=True)

    results = []

    for f in python_files:

        try:
            results_dict = from_file(f, cls, input_dir)

            for name, obj in results_dict.items():
                parse_teams.update_metadata(obj, compile_context.teams_dict)

                # Airflow deps must be set AFTER updating metadata
                airflow_helpers.set_airflow_deps(obj)

                obj.metaData.sourceFile = os.path.relpath(f, compile_context.chronon_root)

                tjson = serializer.thrift_simple_json(obj)

                # Perform validation
                errors = compile_context.validator.validate_obj(obj)

                result = CompiledObj(
                    name=name,
                    obj=obj,
                    file=f,
                    errors=errors if len(errors) > 0 else None,
                    obj_type=cls.__name__,
                    tjson=tjson,
                )
                results.append(result)

                compile_context.compile_status.add_object_update_display(
                    result, cls.__name__
                )

        except Exception as e:
            result = CompiledObj(
                name=None,
                obj=None,
                file=f,
                errors=[e],
                obj_type=cls.__name__,
                tjson=None,
            )

            results.append(result)

            compile_context.compile_status.add_object_update_display(
                result, cls.__name__
            )

    return results


def from_file(file_path: str, cls: type, input_dir: str):

    # this is where the python path should have been set to
    chronon_root = os.path.dirname(input_dir)
    rel_path = os.path.relpath(file_path, chronon_root)

    rel_path_without_extension = os.path.splitext(rel_path)[0]

    module_name = rel_path_without_extension.replace("/", ".")

    conf_type, team_name_with_path = module_name.split(".", 1)
    mod_path = team_name_with_path.replace("/", ".")

    module = importlib.import_module(module_name)

    result = {}

    for var_name, obj in list(module.__dict__.items()):

        if isinstance(obj, cls):

            copied_obj = copy.deepcopy(obj)

            name = f"{mod_path}.{var_name}"
            copied_obj.metaData.name = name
            copied_obj.metaData.team = mod_path.split(".")[0]

            result[name] = copied_obj

    return result
