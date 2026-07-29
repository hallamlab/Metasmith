"""Libraries: the things metasmith stores, typed, on disk.

Five layers, floor first. `resources` is pure value types and imports nothing
from metasmith. `types` is the `{name: Endpoint}` map a `.yml` declares.
`instances` is the `.xgdb` directory of real files and their identities.
`transforms` is the same store viewed as tool definitions. `execution` is the
contract a transform's protocol runs against, and deliberately knows about
none of the others.

`metasmith/__init__.py` exports nothing, so `metasmith.models.libraries` is a
public API rather than an implementation detail: `python_api` re-exports
`Endpoint` and `Transform` from *here* rather than from `models.solver`, and
every transform file in the standard library reaches them that way.

There is deliberately no `__all__`. Eight notebooks under `main/` do
`from metasmith.models.libraries import *`, and two of them call `Path(...)`
without ever importing it -- they get it from this namespace. An `__all__`
naming only the intended API would break them silently, in files no test
executes. So the incidental imports at the bottom are re-exported on purpose:
they were reachable before the split and stay reachable after it.
"""

from __future__ import annotations

# --- the API this package exists to provide -------------------------------
from .resources import Duration, Gpu, GPU_LABEL, Gpus, Resources, Size
from .types import (
    DataTypeLibrary, DataTypeOntologies, DataTypeOntology, yaml_safe_load,
)
from .instances import DataInstance, DataInstanceLibrary, DataInstanceLibraryView
from .transforms import (
    TransformInstance, TransformInstanceLibrary, TransformInstanceLibraryView,
)
from .execution import (
    CONTAINER_ARM, VIRTUAL_ENV_ARM, ContextData, EnvDispatch, ExecutionContext,
    ExecutionFailed, ExecutionResult, ResolveEnvImage,
    _RESERVED_EXPORTS, _validate_exports,
)

# --- pass-throughs callers legitimately reach for by this path -------------
# ContextPath/PathMap moved to models.paths, and Endpoint/Transform/Dependency
# live in models.solver, but the documented import site for all five is here.
from ..paths import ContextPath, PathMap
from ..solver import Dependency, Endpoint, Transform
from ..remote import Logistics, Source, SourceType
from ...env import ContainerDef, Environment, Runtime, Shell
from ...env.dispatch_scan import EnvScan, ScanFile
from ...coms.ipc import GenerateId
from ...coms.terminals import RemoveLeadingIndent
from ...constants import AgentPaths, MODULE_PATH, VERSION
from ...hashing import KeyGenerator
from ...logging import Log
from ...serialization import IsText

# --- incidental, kept reachable -------------------------------------------
# These leaked into the pre-split module namespace as ordinary imports. They
# are not part of the intended API and nothing should grow to depend on them
# through this path; they are re-exported so the split cannot break a
# star-import. See tests/unit/test_module_surface.py for the guard.
import json
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field, replace
from datetime import timedelta
from enum import Enum
from importlib import reload
from pathlib import Path
from typing import Callable, Iterable

import yaml
