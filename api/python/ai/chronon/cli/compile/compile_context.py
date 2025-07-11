import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type

import ai.chronon.cli.compile.parse_teams as teams
from ai.chronon.api.ttypes import GroupBy, Join, MetaData, Model, StagingQuery, Team
from ai.chronon.cli.compile.conf_validator import ConfValidator
from ai.chronon.cli.compile.display.compile_status import CompileStatus
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj
from ai.chronon.cli.compile.serializer import file2thrift
from ai.chronon.cli.logger import get_logger, require
from ai.chronon.orchestration.ttypes import ConfType

logger = get_logger()


@dataclass
class ConfigInfo:
    folder_name: str
    cls: Type
    config_type: Optional[ConfType]


@dataclass
class CompileContext:

    def __init__(self):
        self.chronon_root: str = os.getenv("CHRONON_ROOT", os.getcwd())
        self.teams_dict: Dict[str, Team] = teams.load_teams(self.chronon_root)
        self.compile_dir: str = "compiled"

        self.config_infos: List[ConfigInfo] = [
            ConfigInfo(folder_name="joins", cls=Join, config_type=ConfType.JOIN),
            ConfigInfo(
                folder_name="group_bys",
                cls=GroupBy,
                config_type=ConfType.GROUP_BY,
            ),
            ConfigInfo(
                folder_name="staging_queries",
                cls=StagingQuery,
                config_type=ConfType.STAGING_QUERY,
            ),
            ConfigInfo(folder_name="models", cls=Model, config_type=ConfType.MODEL),
            ConfigInfo(folder_name="teams_metadata", cls=MetaData, config_type=None), # only for team metadata
        ]

        self.compile_status = CompileStatus(use_live=False)

        self.existing_confs: Dict[Type, Dict[str, Any]] = {}
        for config_info in self.config_infos:
            cls = config_info.cls
            self.existing_confs[cls] = self._parse_existing_confs(cls)



        self.validator: ConfValidator = ConfValidator(
            input_root=self.chronon_root,
            output_root=self.compile_dir,
            existing_gbs=self.existing_confs[GroupBy],
            existing_joins=self.existing_confs[Join],
        )

    def input_dir(self, cls: type) -> str:
        """
        - eg., input: group_by class
        - eg., output: root/group_bys/
        """
        config_info = self.config_info_for_class(cls)
        return os.path.join(self.chronon_root, config_info.folder_name)

    def staging_output_dir(self, cls: type = None) -> str:
        """
        - eg., input: group_by class
        - eg., output: root/compiled_staging/group_bys/
        """
        if cls is None:
            return os.path.join(self.chronon_root, self.compile_dir + "_staging")
        else:
            config_info = self.config_info_for_class(cls)
            return os.path.join(
                self.chronon_root,
                self.compile_dir + "_staging",
                config_info.folder_name,
            )

    def output_dir(self, cls: type = None) -> str:
        """
        - eg., input: group_by class
        - eg., output: root/compiled/group_bys/
        """
        if cls is None:
            return os.path.join(self.chronon_root, self.compile_dir)
        else:
            config_info = self.config_info_for_class(cls)
            return os.path.join(
                self.chronon_root, self.compile_dir, config_info.folder_name
            )

    def staging_output_path(self, compiled_obj: CompiledObj):
        """
        - eg., input: group_by with name search.clicks.features.v1
        - eg., output: root/compiled_staging/group_bys/search/clicks.features.v1
        """

        output_dir = self.staging_output_dir(compiled_obj.obj.__class__)  # compiled/joins

        team, rest = compiled_obj.name.split(".", 1)  # search, clicks.features.v1

        return os.path.join(
            output_dir,
            team,
            rest,
        )

    def config_info_for_class(self, cls: type) -> ConfigInfo:
        for info in self.config_infos:
            if info.cls == cls:
                return info

        require(False, f"Class {cls} not found in CONFIG_INFOS")

    def _parse_existing_confs(self, obj_class: type) -> Dict[str, object]:

        result = {}

        output_dir = self.output_dir(obj_class)

        # Check if output_dir exists before walking
        if not os.path.exists(output_dir):
            return result

        for sub_root, _sub_dirs, sub_files in os.walk(output_dir):

            for f in sub_files:

                if f.startswith("."):  # ignore hidden files - such as .DS_Store
                    continue

                full_path = os.path.join(sub_root, f)

                try:
                    obj = file2thrift(full_path, obj_class)

                    if obj:
                        if hasattr(obj, "metaData"):
                            result[obj.metaData.name] = obj
                            compiled_obj = CompiledObj(
                                name=obj.metaData.name,
                                obj=obj,
                                file=obj.metaData.sourceFile,
                                errors=None,
                                obj_type=obj_class.__name__,
                                tjson=open(full_path).read(),
                            )
                            self.compile_status.add_existing_object_update_display(compiled_obj)
                        elif isinstance(obj, MetaData):
                            team_metadata_name = '.'.join(full_path.split('/')[-2:]) # use the name of the file as team metadata won't have name
                            result[team_metadata_name] = obj
                            compiled_obj = CompiledObj(
                                name=team_metadata_name,
                                obj=obj,
                                file=obj.sourceFile,
                                errors=None,
                                obj_type=obj_class.__name__,
                                tjson=open(full_path).read(),
                            )
                            self.compile_status.add_existing_object_update_display(compiled_obj)
                    else:
                        logger.errors(
                            f"Parsed object from {full_path} has no metaData attribute"
                        )

                except Exception as e:
                    print(f"Failed to parse file {full_path}: {str(e)}", e)

        return result
