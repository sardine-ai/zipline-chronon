from dataclasses import dataclass
from typing import Any


@dataclass
class CompiledObj:
    name: str
    obj: Any
    file: str
    error: Exception
    obj_type: str
    tjson: str
