import importlib
import inspect
import os
import subprocess
from dataclasses import dataclass

from ai.chronon.repo import FOLDER_NAME_TO_CLASS, OUTPUT_ROOT
from ai.chronon.repo.compilev2 import extract_and_convert
from ai.chronon.repo.hub_uploader import compute_and_upload_diffs
from ai.chronon.utils import get_mod_and_var_name_from_gc


@dataclass
class ConfigDetails:
    module: str
    variable: str
    file: str
    compiled_file: str
    chronon_root: str
    output_root: str

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
            ) from None

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
        self.chronon_root = _get_chronon_root(self.file)
        self.output_root = f"{self.chronon_root}/{OUTPUT_ROOT}"
        team = path_parts[1]
        path = ".".join(path_parts[2:])
        self.compiled_file = (
            f"{self.output_root}/{obj_type}/{team}/{path}.{self.variable}"
        )


def _get_chronon_root(filepath):
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


def _get_branch(path):
    """
    Get the git branch for the given path.
    """
    try:
        # Get the default branch
        default_branch = (
            subprocess.run(
                ["git", "symbolic-ref", "refs/remotes/origin/HEAD"],
                cwd=os.path.dirname(path),
                capture_output=True,
                text=True,
                check=True,
            )
            .stdout.strip()
            .split("/")[-1]
        )

        # Get the git branch by running git command in the directory
        current_branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=os.path.dirname(path),
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

        if current_branch == default_branch:
            raise RuntimeError(
                f"You're currently on the production branch {default_branch}, please checkout a new branch"
                + "before running to ensure that your changes do not interfere with production."
            )

        else:
            print(f"Identified branch: {current_branch}")
            return current_branch

    except subprocess.CalledProcessError as e:
        raise ValueError(
            f"Failed to get git branch for {path}. Make sure your Chronon directory is in a git repository."
        ) from e


def _compile_and_upload_to_branch(zipline_obj):
    """
    Determines the correct current branch, compiles the repo, uploads the state to the remote branch,
    and returns the branch name
    """
    config_details = ConfigDetails(zipline_obj)
    branch = _get_branch(config_details.chronon_root)
    extract_and_convert(config_details.chronon_root, zipline_obj, config_details.module)
    compute_and_upload_diffs(config_details.output_root, branch)
    return branch


def backfill(self, start_date, end_date, force_recompute=False, plan=False):
    """
    Backfills a Chronon object for a specified date range. Attached to GroupBy, Join and StagingQuery.

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
    _compile_and_upload_to_branch(self)
    print("\n\n TODO -- Implement \n\n")


def deploy(self, date=None, force_recompute=False, plan=False):
    """
    Computes and uploads values for a Zipline for the specified date.
    If there's also a stream job(s) assocaited with the entity, then runs those as well once batch upload succeeds.
    Attached to GroupBy and Join.

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
    _compile_and_upload_to_branch(self)
    print("\n\n TODO -- Implement \n\n")


def info(self, branch=None):
    """
    Prints information about a zipline object, including a link to the ZiplineHub page which
    shows additional information. Attached to GroupBy and Join.

    Args:
        branch: Optional git branch to use for getting object info, if none is provided
            will use the user's dev branch (default: None)

    Returns:
        None

    Raises:
        ValueError: If the object cannot be compiled or info cannot be retrieved
    """
    _compile_and_upload_to_branch(self)
    print("\n\n TODO -- Implement \n\n")


def fetch(self, branch=None):
    return
