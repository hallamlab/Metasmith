"""Workflow: a plan, the task built from it, and the Nextflow it compiles to.

Six modules, roughly in the order a run moves through them. `steps` is a plan's
nodes. `diagnostics` explains a plan that could not be found. `plan` is the
solve itself. `cache_decisions` works out which steps are already done.
`nextflow_codegen` writes the `.nf`. `task` is the bundle that gets staged, and
delegates the three heavy operations to the modules above rather than owning
them.

`nextflow_codegen` is by far the largest, and most of it is one function that
used to be one 800-line method. It reads almost nothing off the task, which is
what let it move out whole.

No `__all__`, for the same reason as `models.libraries`: the pre-split module
namespace is reachable by star-import and this package must not quietly shrink
it. See tests/unit/test_module_surface.py.
"""

from __future__ import annotations

# --- the API this package exists to provide -------------------------------
from .steps import WorkflowStep, WorkflowTarget
from .diagnostics import PlanHint, _diagnose_plan_failure
from .plan import WorkflowPlan
from .cache_decisions import compute_cache_decisions
from .nextflow_codegen import (
    BIND_FILE, METADATA_FILE, NextflowGenContext, NextflowProcessName,
    apply_fs_strategy, prepare_nextflow, _read_env_declarations,
)
from .task import WorkflowTask

# --- pass-throughs callers reach for by this path -------------------------
from ..dag_renderer import DagRenderer, NodeKind
from ..dag_draw import Label, LabelMode
from ..libraries import (
    DataInstance, DataInstanceLibrary, DataInstanceLibraryView, DataTypeLibrary,
    GPU_LABEL, Gpus, TransformInstance, TransformInstanceLibrary,
    TransformInstanceLibraryView,
)
from ..paths import PathMap
from ..remote import Logistics, Source, SourceType
from ..solver import (
    Application, Dependency, Endpoint, Transform, solve_by_mcts,
    Solution as SolverResult,
)
from ...constants import AgentPaths
from ...env import ContainerDef, Environment, Runtime
from ...hashing import KeyGenerator
from ...logging import Log

# --- incidental, kept reachable -------------------------------------------
# Reachable from the pre-split module namespace as ordinary imports; not part
# of the intended API. `Enum` was never used even before the split.
import itertools
import json
import os
from dataclasses import dataclass, field, InitVar
from enum import Enum
from hashlib import md5
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Generator, Iterable, Literal, TypeVar

import yaml
