from dataclasses import dataclass
import os
from typing import Dict, Any, List, Optional, Tuple

from ai.chronon.cli.compile import parse_teams
from ai.chronon.cli.compile.compile_context import CompileContext, ConfigInfo

import ai.chronon.cli.compile.parse_configs as parser
import ai.chronon.cli.compile.serializer as serializer
import ai.chronon.cli.logger as logger

from gen_thrift.common.ttypes import ConfigType

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

        return compile_results

    def _compile_class_configs(self, config_info: ConfigInfo) -> CompileResult:

        compile_result = CompileResult(
            config_info=config_info, obj_dict={}, error_dict={}
        )

        input_dir = self.compile_context.input_dir(config_info.cls)
        output_dir = self.compile_context.output_dir(config_info.cls)

        compiled_objects = parser.from_folder(config_info.cls, input_dir)

        objects, errors = self._write_objects_in_folder(compiled_objects)

        if objects:
            compile_result.obj_dict.update(objects)

        if errors:
            compile_result.error_dict.update(errors)

        success_to_total = (
            f"{len(compile_result.obj_dict)}/{len(compile_result.obj_dict)}"
        )

        logger.info(
            f"Wrote {success_to_total} {config_info.cls.__name__}-s to {output_dir}. "
            f"With {len(compile_result.error_dict)} errors."
        )

        return compile_result

    def _write_objects_in_folder(
        self, compiled_objects: List[parser.CompiledObj]
    ) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:

        error_dict = {}
        object_dict = {}

        for co in compiled_objects:

            if co.obj:

                errors = self._write_object(co.obj)

                parse_teams.update_metadata(co.obj, self.compile_context.teams_dict)

                if errors:
                    error_dict[co.name] = errors

                    for error in errors:
                        logger.error(f"Error processing {co.name}: {error}")

                else:
                    object_dict[co.name] = co.obj

            else:
                error_dict[co.file] = co.error
                logger.error(f"Error processing file {co.file}: {co.error}")

        return object_dict, error_dict

    def _write_object(self, obj: Any) -> Optional[List[str]]:

        errors = self.compile_context.validator.validate_obj(obj)
        if errors:
            return errors

        output_path = self.compile_context.output_path(obj)

        folder = os.path.dirname(output_path)

        if not os.path.exists(folder):
            os.makedirs(folder)

        with open(output_path, "w") as f:
            f.write(serializer.thrift_simple_json(obj))
