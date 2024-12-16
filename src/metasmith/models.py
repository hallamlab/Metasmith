from pathlib import Path
from dataclasses import dataclass, field
from typing import Callable

class NotImplementedException(Exception):
    pass

@dataclass
class ExecutionContext:
    pass

@dataclass
class ExecutionResult:
    pass

@dataclass
class TransformReference:
    container: str
    protocol: Callable[[ExecutionContext], ExecutionResult]
    input_signature: dict[str, set[str]] = field(default_factory=dict)
    output_signature: dict[str, set[str]] = field(default_factory=dict)

@dataclass
class ArtifactPointer:
    platform: str
    url: str

# abstract
class DataManager:
    def __init__(self, config: dict) -> None:
        pass

    def Prepare(self, manifest: list[ArtifactPointer]):
        raise NotImplementedException()
