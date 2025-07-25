import os
import shutil
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import ai.chronon.cli.compile.display.compiled_obj
import ai.chronon.cli.compile.parse_configs as parser
import ai.chronon.cli.logger as logger
from ai.chronon.cli.compile import serializer
from ai.chronon.cli.compile.compile_context import CompileContext, ConfigInfo
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj
from ai.chronon.cli.compile.display.console import console
from ai.chronon.cli.compile.parse_teams import merge_team_execution_info
from ai.chronon.orchestration.ttypes import ConfType
from ai.chronon.types import MetaData

logger = logger.get_logger()


@dataclass
class CompileResult:
    config_info: ConfigInfo
    obj_dict: Dict[str, Any]
    error_dict: Dict[str, List[BaseException]]


class Compiler:

    def __init__(self, compile_context: CompileContext):
        self.compile_context = compile_context

    def compile(self) -> Dict[ConfType, CompileResult]:

        config_infos = self.compile_context.config_infos

        compile_results = {}

        for config_info in config_infos:
            configs = self._compile_class_configs(config_info)

            compile_results[config_info.config_type] = configs
        self._compile_team_metadata()

        # check if staging_output_dir exists
        staging_dir = self.compile_context.staging_output_dir()
        if os.path.exists(staging_dir):
            # replace staging_output_dir to output_dir
            output_dir = self.compile_context.output_dir()
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)
            shutil.move(staging_dir, output_dir)
        else:
            print(
                f"Staging directory {staging_dir} does not exist. "
                "Happens when every chronon config fails to compile or when no chronon configs exist."
            )

        # TODO: temporarily just print out the final results of the compile until live fix is implemented:
        #  https://github.com/Textualize/rich/pull/3637
        console.print(self.compile_context.compile_status.render())

        return compile_results

    def _compile_team_metadata(self):
        """
        Compile the team metadata and return the compiled object.
        """
        teams_dict = self.compile_context.teams_dict
        for team in teams_dict:
            m = MetaData()
            merge_team_execution_info(m, teams_dict, team)

            tjson = serializer.thrift_simple_json(m)
            name = f"{team}.{team}_team_metadata"
            result = CompiledObj(
                name=name,
                obj=m,
                file=name,
                errors=None,
                obj_type=MetaData.__name__,
                tjson=tjson,
            )
            self._write_object(result)
            self.compile_context.compile_status.add_object_update_display(result, MetaData.__name__)

        # Done writing team metadata, close the class
        self.compile_context.compile_status.close_cls(MetaData.__name__)

    def _compile_class_configs(self, config_info: ConfigInfo) -> CompileResult:

        compile_result = CompileResult(
            config_info=config_info, obj_dict={}, error_dict={}
        )

        input_dir = self.compile_context.input_dir(config_info.cls)

        compiled_objects = parser.from_folder(
            config_info.cls, input_dir, self.compile_context
        )

        objects, errors = self._write_objects_in_folder(compiled_objects)

        if objects:
            compile_result.obj_dict.update(objects)

        if errors:
            compile_result.error_dict.update(errors)

        self.compile_context.compile_status.close_cls(config_info.cls.__name__)

        return compile_result

    def _write_objects_in_folder(
        self,
        compiled_objects: List[ai.chronon.cli.compile.display.compiled_obj.CompiledObj],
    ) -> Tuple[Dict[str, Any], Dict[str, List[BaseException]]]:

        error_dict = {}
        object_dict = {}

        for co in compiled_objects:

            if co.obj:

                if co.errors:
                    error_dict[co.name] = co.errors

                    for error in co.errors:
                        self.compile_context.compile_status.print_live_console(
                            f"Error processing conf {co.name}: {error}"
                        )
                        traceback.print_exception(
                            type(error), error, error.__traceback__
                        )

                else:
                    self._write_object(co)
                    object_dict[co.name] = co.obj
            else:
                error_dict[co.file] = co.errors

                self.compile_context.compile_status.print_live_console(
                    f"Error processing file {co.file}: {co.errors}"
                )
                for error in co.errors:
                    traceback.print_exception(type(error), error, error.__traceback__)

        return object_dict, error_dict

    def _write_object(self, compiled_obj: CompiledObj) -> Optional[List[BaseException]]:
        output_path = self.compile_context.staging_output_path(compiled_obj)

        folder = os.path.dirname(output_path)

        if not os.path.exists(folder):
            os.makedirs(folder)

        with open(output_path, "w") as f:
            f.write(compiled_obj.tjson)
