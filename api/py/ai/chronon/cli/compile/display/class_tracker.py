import difflib
from typing import Dict, List, Any
from rich.text import Text

from ai.chronon.cli.compile.display.compiled_obj import CompiledObj
from ai.chronon.cli.compile.display.diff_result import DiffResult


class ClassTracker:
    """
    Tracker object per class - Join, StagingQuery, GroupBy etc
    """

    def __init__(self):
        self.existing_objs: Dict[str, CompiledObj] = {}  # name to obj
        self.files_to_obj: Dict[str, List[Any]] = {}
        self.files_to_errors: Dict[str, Exception] = {}
        self.new_objs: Dict[str, CompiledObj] = {}  # name to obj
        self.diff_result = DiffResult()
        self.deleted_names: List[str] = []

    def add_existing(self, obj: CompiledObj) -> None:
        self.existing_objs[obj.name] = obj

    def add(self, compiled: CompiledObj) -> None:

        if compiled.error:

            if compiled.file not in self.files_to_errors:
                self.files_to_errors[compiled.file] = []

            self.files_to_errors[compiled.file].append(compiled.error)

        else:
            if compiled.file not in self.files_to_obj:
                self.files_to_obj[compiled.file] = []

            self.files_to_obj[compiled.file].append(compiled.obj)

            self.new_objs[compiled.name] = compiled
            self._update_diff(compiled)

    def _update_diff(self, compiled: CompiledObj) -> None:
        if compiled.name in self.existing_objs:

            existing_json = self.existing_objs[compiled.name].tjson
            new_json = compiled.tjson

            if existing_json != new_json:

                diff = difflib.unified_diff(
                    existing_json.splitlines(keepends=True),
                    new_json.splitlines(keepends=True),
                    n=2,
                )

                print(f"Updated object: {compiled.name} in file {compiled.file}")
                print("".join(diff))
                print("\n")

                self.diff_result.updated.append(compiled.name)

        else:
            self.diff_result.added.append(compiled.name)

    def close(self) -> None:
        self.closed = True
        self.recent_file = None
        self.deleted_names = list(self.existing_objs.keys() - self.new_objs.keys())

    def to_status(self) -> Text:
        text = Text()

        if self.existing_objs:
            text.append(
                f"  Parsed {len(self.existing_objs)} previously compiled objects.\n"
            )

        if self.files_to_obj:
            text.append("  Compiled ")
            text.append(f"{len(self.new_objs)} ", style="bold green")
            text.append(f"objects from ")
            text.append(f"{len(self.files_to_obj)} ", style="bold green")
            text.append("files.\n")

        if self.files_to_errors:
            text.append("  Failed to compile ")
            text.append(f"{len(self.files_to_errors)} ", style="red")
            text.append("files.\n")

        return text

    def to_errors(self) -> Text:
        text = Text()

        if self.files_to_errors:
            for file, error in self.files_to_errors.items():
                text.append("  ERROR ", style="bold red")
                text.append(f"- {file}: {error[0]}\n")

        return text

    # doesn't make sense to show deletes until the very end of compilation
    def diff(self) -> Text:
        return self.diff_result.render(deleted_names=self.deleted_names)
