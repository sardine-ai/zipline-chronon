from dataclasses import dataclass
import os
import shutil
import traceback
from typing import Dict, Any, List, Optional, Tuple

from ai.chronon.cli.compile.compile_context import CompileContext, ConfigInfo

from ai.chronon.cli.compile.display.class_tracker import ClassTracker
import ai.chronon.cli.compile.display.compiled_obj
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj
import ai.chronon.cli.compile.parse_configs as parser
import ai.chronon.cli.logger as logger

from ai.chronon.api.common.ttypes import ConfigType

logger = logger.get_logger()


@dataclass
class CompileResult:
    config_info: ConfigInfo
    obj_dict: Dict[str, Any]
    error_dict: Dict[str, List[str]]


class Compiler:

    def __init__(self, compile_context: CompileContext):
        self.compile_context = compile_context

    def compile(
        self, compile_context: CompileContext
    ) -> Dict[ConfigType, CompileResult]:

        config_infos = compile_context.config_infos

        compile_results = {}

        for config_info in config_infos:

            compile_results[config_info.config_type] = self._compile_class_configs(
                config_info
            )

        # replace staging_output_dir to output_dir
        staging_dir = self.compile_context.staging_output_dir()
        output_dir = self.compile_context.output_dir()
        shutil.rmtree(output_dir)
        shutil.move(staging_dir, output_dir)

        return compile_results

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
    ) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:

        error_dict = {}
        object_dict = {}

        for co in compiled_objects:

            if co.obj:

                errors = self._write_object(co)

                if errors:
                    error_dict[co.name] = errors

                    for error in errors:
                        print(f"\nError processing conf {co.name}: {error}")
                        traceback.print_exception(
                            type(error), error, error.__traceback__
                        )
                        print("\n")

                else:
                    object_dict[co.name] = co.obj

            else:
                error_dict[co.file] = co.error

                print(f"\nError processing file {co.file}: {co.error}")
                traceback.print_exception(
                    type(co.error), co.error, co.error.__traceback__
                )
                print("\n")

        return object_dict, error_dict

    def _write_object(self, compiled_obj: CompiledObj) -> Optional[List[str]]:

        obj = compiled_obj.obj

        errors = self.compile_context.validator.validate_obj(obj)
        if errors:
            return errors

        output_path = self.compile_context.staging_output_path(obj)

        folder = os.path.dirname(output_path)

        if not os.path.exists(folder):
            os.makedirs(folder)

        with open(output_path, "w") as f:
            f.write(compiled_obj.tjson)
