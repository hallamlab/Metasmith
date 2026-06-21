"""The `env` module: the sealed boundary for runtime selection and routing.

Nothing outside this package should name a concrete runtime (DOCKER /
APPTAINER / MAMBA) or construct a relay shell. Callers hold an
`Environment` and a `Runtime` selector; the per-runtime details live here.

Back-compat aliases (`Container`, `ContainerRuntime`) are re-exported for the
duration of the carve and will be removed once all call sites migrate to the
`Environment`/`Runtime` names.
"""

from .environment import Environment, Runtime
from ._shell import Shell

# The relay client is owned by the env module; it is re-exported here so the
# one historical public re-export (python_api) can redirect through env
# rather than reaching into coms directly.
from ..coms.via_file_watcher import RemoteShell

# Transitional aliases — call sites are migrating from the old names.
Container = Environment
ContainerRuntime = Runtime

__all__ = [
    "Environment", "Runtime", "Shell", "RemoteShell",
    "Container", "ContainerRuntime",
]
