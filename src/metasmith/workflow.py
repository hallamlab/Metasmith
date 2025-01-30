import itertools
import re
import yaml
from pathlib import Path
from dataclasses import dataclass, field
import numpy as np

from .models import TransformReference
from .solver import Namespace, Transform, Endpoint, Solve
from .transforms import LoadTransform
from .logging import Log

class Workspace:
    def __init__(self, home: Path) -> None:
        self._transforms = home/"metasmith_definitions/transforms"
        pass
    
    def GetTransforms(self):
        return self._transforms


def LoadTransforms(workspace: Workspace, namespace: Namespace):
    transforms_definitions: list[str] = []
    for d in (workspace.TRANSFORMS).rglob("*.py"):
        if d.stem.startswith("_"): continue
        transforms_definitions.append(d)

    transform_references = [LoadTransform(d) for d in transforms_definitions]
    transforms: list[Transform] = []
    for r in transform_references:
        tr = Transform(NS)
        for k, v in r.input_signature.items():
            tr.AddRequirement(v)
        for k, v in r.output_signature.items():
            tr.AddProduct(v)
        transforms.append(tr)


def Run(home: Path, request_file: Path):
    solutions = GeneratePlan(home, request_file)
    if solutions is None: return # error has ocurred

    for step in solutions.dependency_plan:
        Log.Info(f"{step}")
        Log.Info("")

