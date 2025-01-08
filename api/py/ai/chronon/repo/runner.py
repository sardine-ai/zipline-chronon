import os
import importlib
import inspect
from dataclasses import dataclass
from ai.chronon.repo.compilev2 import extract_and_convert
from ai.chronon.utils import get_mod_and_var_name_from_gc
from ai.chronon.repo import FOLDER_NAME_TO_CLASS, OUTPUT_ROOT


@dataclass
class ConfigDetails:
    module: str
    variable: str
    file: str
    compiled_file: str
    chronon_root: str

    def __init__(self, obj):

        # Get object type from FOLDER_NAME_TO_CLASS
        try:
            obj_type = next(
                k for k, v in FOLDER_NAME_TO_CLASS.items() if isinstance(obj, v)
            )
        except StopIteration:
            valid_types = [cls.__name__ for cls in FOLDER_NAME_TO_CLASS.values()]
            raise ValueError(
                f"Can only run one of {valid_types}, got {type(obj).__name__}"
            )

        # Get module and variable name
        self.module, self.variable = get_mod_and_var_name_from_gc(obj, obj_type)

        if self.module is None or self.variable is None:
            raise ValueError("Could not determine module and variable name for object")

        # Get file path
        module = importlib.import_module(self.module)
        self.file = inspect.getmodule(module).__file__
        if not self.file:
            raise ValueError(
                f"""
                                Could not determine file location for module {self.module}, {self.variable}.\n
                                Runner currently only supports working on files saved within a valid Chronon
                                root directory.\n Make sure you have your Zipline python files within the
                                right directory, and then you can import them to your desired runtime.
                             """
            )

        # Validate module path
        path_parts = self.module.split(".")
        if path_parts[0] != obj_type:
            raise ValueError(
                f"Expected module path to start with '{obj_type}', got {self.module}"
            )

        # Get chronon root and build compiled file path
        self.chronon_root = get_chronon_root(self.file)
        team = path_parts[1]
        path = ".".join(path_parts[2:])
        self.compiled_file = f"{self.chronon_root}/{OUTPUT_ROOT}/{obj_type}/{team}/{path}.{self.variable}"


def get_chronon_root(filepath):
    """
    Infer chronon root from a filepath to a Chronon object
    """
    target_dirs = FOLDER_NAME_TO_CLASS.keys()

    current_path = os.path.dirname(os.path.abspath(filepath))

    while current_path != os.path.dirname(current_path):  # Stop at root directory
        dir_name = os.path.basename(current_path)
        if dir_name in target_dirs:
            return os.path.dirname(current_path)
        current_path = os.path.dirname(current_path)

    raise ValueError(
        f"{filepath} is not within a valid Chronon root directory, containing {target_dirs} subdirs."
    )


def backfill(zipline_obj, start_date, end_date, force_recompute=False, plan=False):
    """
    Backfills a Chronon object for a specified date range.

    Args:
        zipline_obj: The Chronon object (GroupBy, Join, StagingQuery) to backfill
        start_date: Start date for the backfill period
        end_date: End date for the backfill period
        force_recompute: If True, recomputes data even if it already exists (default: False)
        plan: If True, only shows execution plan without running backfill (default: False)

    Returns:
        None

    Raises:
        ValueError: If the object cannot be compiled or backfilled
    """
    config_details = ConfigDetails(zipline_obj)
    # module_file, compiled_file, chronon_root = get_target_compiled_file(zipline_obj)
    extract_and_convert(config_details.chronon_root, zipline_obj, config_details.module)
    # TODO: actually kick off backfill for compiled_file
    print("\n\n TODO -- Implement \n\n")


def upload(zipline_obj, date, force_recompute=False, plan=False):
    """
    Computes and uploads values for a Zipline for the specified date.

    Args:
        zipline_obj: The Chronon object (GroupBy, Join) to upload. If join is provided, then
          runs upload for all JoinParts.
        date: The date to upload data for (default: 2 days ago UTC)
        force_recompute: If True, recomputes data even if it already exists (default: False)
        plan: If True, only shows execution plan without running upload (default: False)

    Returns:
        None

    Raises:
        ValueError: If the object cannot be compiled or uploaded
    """
    print("\n\n TODO -- Implement \n\n")


def stream(zipline_obj):
    """
    Starts a streaming job for this Zipline object.

    Args:
        zipline_obj: The Chronon object (GroupBy, Join) to upload. If join is provided, then
          runs streaming job for all JoinParts.

    Returns:
        None

    Raises:
        ValueError: If the object cannot be compiled or streamed
    """
    print("\n\n TODO -- Implement \n\n")


def info(zipline_obj, branch=None):
    """
    Prints information about a zipline object, including a link to the ZiplineHub page which
    shows additional information

    Args:
        zipline_obj: The Chronon object (GroupBy, Join, StagingQuery) to get info for
        branch: Optional git branch to use for getting object info, if none is provided
            will use the user's dev branch (default: None)

    Returns:
        None

    Raises:
        ValueError: If the object cannot be compiled or info cannot be retrieved
    """
    print("\n\n TODO -- Implement \n\n")
