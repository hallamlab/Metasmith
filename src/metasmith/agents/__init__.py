from __future__ import annotations

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

from .collect import (
    CollectResults,
    PublishCachedProducts,
    _published_index,
    _published_path,
)
from .runner import (
    CheckWorkflow, RunWorkflow, StageWorkflow, _extract_nxf_task_metadata,
)

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
