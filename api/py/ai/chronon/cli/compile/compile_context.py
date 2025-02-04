from dataclasses import dataclass
from logging import error
import os
from typing import Any, Dict, List, Type

from ai.chronon.api.common.ttypes import ConfigType
from ai.chronon.api.ttypes import GroupBy, Join, StagingQuery, Team
from ai.chronon.cli.compile.serializer import file2thrift
from ai.chronon.cli.compile.conf_validator import ConfValidator
import ai.chronon.cli.compile.parse_teams as teams
from ai.chronon.cli.logger import require, get_logger
from ai.chronon.model import Model


logger = get_logger()


@dataclass
class ConfigInfo:
    folder_name: str
    cls: Type
    config_type: ConfigType


@dataclass
class CompileContext:

    def __init__(self):
        self.chronon_root: str = os.getenv("CHRONON_ROOT", os.getcwd())
        self.teams_dict: Dict[str, Team] = teams.load_teams(self.chronon_root)
        self.compile_dir: str = "compiled"

        self.config_infos: List[ConfigInfo] = [
            ConfigInfo(folder_name="joins", cls=Join, config_type=ConfigType.JOIN),
            ConfigInfo(
                folder_name="group_bys",
                cls=GroupBy,
                config_type=ConfigType.GROUP_BY,
            ),
            ConfigInfo(
                folder_name="staging_queries",
                cls=StagingQuery,
                config_type=ConfigType.STAGING_QUERY,
            ),
            ConfigInfo(folder_name="models", cls=Model, config_type=ConfigType.MODEL),
        ]

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
        - input: group_by class
        - output: root/group_bys/
        """
        config_info = self.config_info_for_class(cls)
        return os.path.join(self.chronon_root, config_info.folder_name)

    def output_dir(self, cls: type) -> str:
        """
        - input: group_by class
        - output: root/compiled/group_bys/
        """
        config_info = self.config_info_for_class(cls)
        return os.path.join(
            self.chronon_root, self.compile_dir, config_info.folder_name
        )

    def output_path(self, obj: Any):
        """
        - input: group_by with name search.clicks.features.v1
        - output: root/compiled/group_bys/search/clicks.features.v1
        """

        output_dir = self.output_dir(obj.__class__)  # compiled/joins

        team, rest = obj.metaData.name.split(".", 1)  # search, clicks.features.v1

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

        for sub_root, sub_dirs, sub_files in os.walk(output_dir):

            for f in sub_files:

                if not f.startswith("."):  # ignore hidden files - such as .DS_Store
                    continue

                full_path = os.path.join(sub_root, f)

                try:
                    obj = file2thrift(full_path, obj_class)

                    if obj and hasattr(obj, "metaData"):
                        result[obj.metaData.name] = obj

                except Exception as e:
                    logger.error(f"Failed to parse file {full_path}", e)

        return result
