"""Agents: a metasmith installation on another host, and the code that runs there.

The package divides on which side of the wire the code executes.

Client side, in the order a caller uses them: `targets` states what is wanted,
`agent` is the handle and the deploy that makes a host into an agent, and
`workflow_ops` / `run_control` are mixed into `Agent` to give it the verbs --
generate, stage, run, then wait, tail, cancel. `shell` is the held-open
connection all three reach for; `gpu` and `portability` are the two preflights
that refuse a run the far side cannot honour, and `images` is the third, which
only reports -- plus the pre-flight fetch a caller asks for by name.

Agent side, launched by the staged agent from inside its container: `runner`
holds the three RPC entry points and `collect` reassembles a finished run.

The two sides meet at three shared names -- `RunWorkflow`, `StageWorkflow`,
`CheckWorkflow` -- which exist as both an `Agent` method and a free function.
The re-exports below must keep the FREE functions winning those names, because
`coms/api.py` imports them by bare name from this package. That is also why
`runner` is imported last.

No `__all__`, for the same reason as `models.libraries` and `models.workflow`:
the pre-split module namespace is reachable by star-import and this package must
not quietly shrink it. See tests/unit/test_module_surface.py.
"""

from __future__ import annotations

# --- client side ----------------------------------------------------------
from .shell import AgentShell
from .targets import ResourceOverrides, TargetBuilder, TargetSpec
from .spec import Spec
from .templates import Template
from .gpu import (
    GPU_LABEL, GpuRequirementError, _GPU_BEFORE_SCRIPT, _plan_gpu_requests,
    _read_gpu_manifest, _render_gpu_config,
)
from .portability import (
    EnvPortabilityError, _check_env_portability, _read_env_manifest,
    _read_env_manifest_doc,
)
from .images import (
    ImageMaterialiseError, _check_image_store, _manifest_images,
    _materialise_images, _tool_environment_for,
)
from .workflow_ops import GetNxfConfigPresets
from .agent import Agent

# --- agent side -----------------------------------------------------------
# Last, and by design: these free functions must win the three names they share
# with `Agent` methods. `coms/api.py` imports them from here by bare name.
from .collect import CollectResults, _published_index, _published_path
from .runner import (
    CheckWorkflow, RunWorkflow, StageWorkflow, _extract_nxf_task_metadata,
)

# --- pass-throughs callers reach for by this path -------------------------
# `python_api`, `bin/sbatch`, `bin/squeue` and `bin/scancel` all import
# AgentPaths from here rather than from constants.
from ..constants import AgentPaths, CONTAINER_TAG, MODULE_PATH, VERSION
from ..coms.terminals import (
    IDLE_TIMEOUT, LiveShell, PROBE_TIMEOUT, RemoveLeadingIndent,
    SSH_CONNECT_TIMEOUT, ShellResult,
)
from ..env import ContainerDef, Environment, Runtime
from ..hashing import KeyGenerator
from ..logging import Log
from ..models.libraries import (
    DataInstance, DataInstanceLibrary, DataInstanceLibraryView, DataTypeLibrary,
    Gpu, Gpus, Resources, Size, TransformInstance, TransformInstanceLibrary,
    TransformInstanceLibraryView,
)
from ..models.lineage import LinPayload
from ..models.paths import PathMap
from ..models.remote import GlobusSource, Logistics, Source, SourceType, SshSource
from ..models.solver import Dependency, Endpoint, Solution, Transform
from ..models.workflow import (
    BIND_FILE, METADATA_FILE, NextflowGenContext, WorkflowPlan, WorkflowStep,
    WorkflowTarget, WorkflowTask,
)
from ..serialization import StdTime

# --- incidental, kept reachable -------------------------------------------
# Reachable from the pre-split module namespace as ordinary imports; not part of
# the intended API. `deque` was never used even before the split.
import json
import os
import re
import shutil
import tempfile
from collections import deque
from dataclasses import dataclass, field
from hashlib import md5
from pathlib import Path
from typing import Callable, Iterable, Literal

import pandas as pd
import yaml
