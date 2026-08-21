from __future__ import annotations

from .steps import WorkflowStep, WorkflowTarget
from .diagnostics import PlanHint, _diagnose_plan_failure
from .plan import WorkflowPlan
from .cache_decisions import compute_cache_decisions
from .leaf_identity import restat_leaf_ids
from .nextflow_codegen import (
    BIND_FILE, METADATA_FILE, NextflowGenContext, NextflowProcessName,
    apply_fs_strategy, prepare_nextflow, _read_env_declarations,
)
from .task import WorkflowTask

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
