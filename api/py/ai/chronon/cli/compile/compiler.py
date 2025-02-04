from dataclasses import dataclass
import os
from typing import Dict, Any, List, Optional

from ai.chronon.cli.compile import parse_teams
from ai.chronon.cli.compile.compile_context import CompileContext, ConfigInfo

import ai.chronon.cli.compile.parse_configs as parser
import ai.chronon.cli.compile.serializer as serializer
import ai.chronon.cli.logger as logger

from ai.chronon.api.common.ttypes import ConfigType

import time

logger = logger.get_logger()


@dataclass
class CompileResult:
    config_info: ConfigInfo
    obj_dict: Dict[str, Any]
    error_dict: Dict[str, List[str]]


def main(compile_context: CompileContext) -> Dict[ConfigType, CompileResult]:

    config_infos = compile_context.config_infos

    compile_results = {}

    from rich.console import Console

    console = Console()

    with console.status("[bold green]Loading...", spinner="dots") as status:

        for config_info in config_infos:

            compile_result = CompileResult(
                config_info=config_info, obj_dict={}, error_dict={}
            )

            compile_results[config_info.config_type] = compile_result

            input_dir = compile_context.input_dir(config_info.cls)
            output_dir = compile_context.output_dir(config_info.cls)

            compiled_objects = parser.from_folder(config_info.cls, input_dir)

            for co in compiled_objects:

                if co.obj:

                    errors = _write_config_or_return_errors(
                        co.name, co.obj, config_info, compile_context
                    )

                    parse_teams.update_metadata(co.obj, compile_context.teams_dict)

                    if errors:
                        compile_result.error_dict[co.name] = errors

                        for error in errors:
                            logger.error(f"Error processing {co.name}: {error}")

                    else:
                        compile_result.obj_dict[co.name] = co.obj

                else:
                    compile_result.error_dict[co.file] = co.error
                    logger.error(f"Error processing file {co.file}: {co.error}")

            success_to_total = (
                f"{len(compile_result.obj_dict)}/{len(compile_result.obj_dict)}"
            )

            logger.info(
                f"Wrote {success_to_total} {config_info.cls.__name__}-s to {output_dir}. "
                f"With {len(compile_result.error_dict)} errors."
            )

    return compile_results


def _write_config_or_return_errors(
    name: str, obj: Any, config_info: ConfigInfo, compile_context: CompileContext
) -> Optional[List[str]]:

    errors = compile_context.validator.validate_obj(obj)
    if errors:
        return errors

    logger.debug(f"{name}: \n  {obj}, \n  {config_info}")
    output_path = compile_context.output_path(obj)

    folder = os.path.dirname(output_path)

    if not os.path.exists(folder):
        os.makedirs(folder)

    with open(output_path, "w") as f:
        f.write(serializer.thrift_simple_json(obj))
