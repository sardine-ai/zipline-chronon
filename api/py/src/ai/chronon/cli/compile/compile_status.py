from dataclasses import dataclass
from typing import Any, Dict, List, Set, Type
from rich.live import Live
from rich.text import Text
from rich.console import Console
from collections import OrderedDict, defaultdict


@dataclass
class CompiledObj:
    name: str
    obj: Any
    file: str
    error: Exception


@dataclass
class DiffResult:
    added: List[str]
    updated: List[str]
    deleted: List[str]

    @staticmethod
    def build_from(existing: Dict[str, Any], new: Dict[str, Any]) -> "DiffResult":

        existing_names = existing.keys()
        new_names = new.keys()

        added = new_names - existing_names
        deleted = existing_names - new_names
        common = existing_names & new_names

        updated = [name for name in common if existing[name] != new[name]]

        return DiffResult(added, updated, deleted)

    def render(self, indent="  ", show_deletes=False) -> Text:

        def added_signage():
            return Text("Added", style="dim")

        def updated_signage():
            return Text("Updated", style="dim")

        def deleted_signage():
            return Text("Deleted", style="dim")

        added = [(added_signage(), name) for name in self.added]

        udpated = [(updated_signage(), name) for name in self.updated]

        deleted = [(deleted_signage(), name) for name in self.deleted]

        result_order = added + udpated + (deleted if show_deletes else [])
        result_order = sorted(result_order, key=lambda t: t[1])

        text = Text()
        for signage, name in result_order:
            text.append(indent)
            text.append(signage)
            text.append(" ")
            text.append(name)
            text.append("\n")

        return text


class ClassTracker:
    """
    Tracker object per class - Join, StagingQuery, GroupBy etc
    """

    def __init__(self):
        self.existing_objs: Dict[str, Any] = {}  # name to obj
        self.files_to_obj: Dict[str, List[Any]] = {}
        self.files_to_errors: Dict[str, Exception] = {}
        self.new_objs: Dict[str, Any] = {}  # name to obj
        self.closed: bool = False
        self.recent_file: str = None

    def add_existing(self, obj) -> None:
        self.existing_files[obj.metaData.name] = obj

    def add(self, compiled: CompiledObj) -> None:
        self.recent_file = compiled.file

        if compiled.error:

            if compiled.file not in self.files_to_errors:
                self.files_to_errors[compiled.file] = []

            self.files_to_errors[compiled.file].append(compiled.error)

        else:
            if compiled.file not in self.files_to_obj:
                self.files_to_obj[compiled.file] = []

            self.files_to_obj[compiled.file].append(compiled.obj)

            self.new_objs[compiled.obj.metaData.name] = compiled.obj

    def close(self) -> None:
        self.closed = True
        self.recent_file = None

    def to_status(self) -> Text:
        text = Text()

        if self.existing_objs:
            text.append(
                f"Parsed {len(self.existing_objs)} previously compiled objects."
            )

        if self.files_to_obj:
            text.append("\n  Compiled ")
            text.append(f"{len(self.new_objs)} ", style="bold green")
            text.append(f"objects from ")
            text.append(f"{len(self.files_to_obj)} ", style="bold green")
            text.append("files. ")

        if self.files_to_errors:
            text.append("\n  Failed to compile ")
            text.append(f"{len(self.files_to_errors)} ", style="red")
            text.append("files. ")

        if not self.closed and self.recent_file:
            text.append("\n  Compiling file - ", style="dim")
            text.append(self.recent_file, style="blue")
        else:
            text.append("\n")

        return text

    def to_errors(self) -> Text:
        text = Text()

        if self.files_to_errors:
            for file, error in self.files_to_errors.items():
                text.append("\n  ERROR ", style="bold red")
                text.append(f"- {file}: {error[0]}")
            text.append("\n")

        return text

    # doesn't make sense to show deletes until the very end of compilation
    def diff(self, indent="  ", show_deletes=False) -> Text:
        return DiffResult.build_from(self.existing_objs, self.new_objs).render(
            indent, show_deletes
        )


class CompileStatus:
    """
    Uses rich ui - to consolidate and sink the overview of the compile process to the bottom.
    """

    def __init__(self):
        self.console = Console()
        self.cls_to_tracker: Dict[str, ClassTracker] = OrderedDict()
        self.running: bool = True
        self.live = Live("", console=self.console, refresh_per_second=10)

    def add_object_update_display(self, compiled: CompiledObj, obj_type: str) -> None:

        if obj_type not in self.cls_to_tracker:
            self.cls_to_tracker[obj_type] = ClassTracker()

        self.cls_to_tracker[obj_type].add(compiled)

        self._update_display()

    def add_existing_object_update_display(
        self, existing_obj: Any, obj_type: str
    ) -> None:

        if obj_type not in self.cls_to_tracker:
            self.cls_to_tracker[obj_type] = ClassTracker()

        self.cls_to_tracker[obj_type].add_existing(existing_obj)

        self._update_display()

    def close_cls(self, obj_type: str) -> None:
        self.cls_to_tracker[obj_type].close()
        self._update_display()

    def close(self) -> None:
        self.running = False
        self._update_display()

    def _update_display(self) -> Text:
        text = Text()

        # Print errors on the top
        for obj_type, tracker in self.cls_to_tracker.items():
            errors = tracker.to_errors()
            if errors:
                text.append(f"\nErrors parsing ")
                text.append(f"{obj_type}-s", style="bold blue")
                text.append(f":")
                text.append(errors)
        text.append("\n")

        for obj_type, tracker in self.cls_to_tracker.items():
            status = tracker.to_status()
            if status:
                text.append(f"{obj_type}-s: ", style="cyan")
                text.append(status)

            diff = tracker.diff()
            if diff:
                text.append(diff)

            text.append("\n")

        if not self.running:
            text.append("Done compiling.", style="italic")

        self.live.update(text)

    def __enter__(self):
        self.live.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.live.__exit__(exc_type, exc_val, exc_tb)


# Example usage:
if __name__ == "__main__":
    import time

    class MetaData:
        def __init__(self, name: str):
            self.name = name

    # Example objects for testing
    class TestObj:
        def __init__(self, name: str):
            self.metaData = MetaData(name)

    class AnotherObj:
        def __init__(self, name: str):
            self.metaData = MetaData(name)

    with CompileStatus() as status:
        # Simulate some compilations
        objs = [
            CompiledObj("test1", TestObj("t1"), "file1.py", None),
            CompiledObj("test2", TestObj("t2"), "file2.py", None),
            CompiledObj("test3", None, "file3.py", ValueError("Invalid syntax")),
            CompiledObj("test4", AnotherObj("a1"), "file4.py", None),
            CompiledObj("test5", None, "file5.py", ImportError("Module not found")),
        ]

        for obj in objs[:3]:
            status.add_object_update_display(obj, TestObj.__name__)
            time.sleep(0.1)  # Simulate processing time

        status.close_cls(TestObj.__name__)

        for obj in objs[3:]:
            status.add_object_update_display(obj, AnotherObj.__name__)
            time.sleep(0.1)  # Simulate processing time

        status.close_cls(AnotherObj.__name__)
        status.close()

    # First create some sample data to compare
    existing_data = {
        "config.yaml": "old content",
        "script.py": "def hello(): pass",
        "readme.md": "# Project",
        "obsolete.txt": "will be deleted",
    }

    new_data = {
        "config.yaml": "new content",  # This will be marked as updated
        "script.py": "def hello(): pass",  # This remains unchanged
        "readme.md": "# Updated Project",  # This will be marked as updated
        "new_file.py": "print('hello')",  # This will be marked as added
        # obsolete.txt is missing - will be marked as deleted
    }

    diff = DiffResult.build_from(existing_data, new_data)

    console = Console()

    # console.print(diff.render())

    # console.print(diff.render(show_deletes=True))
