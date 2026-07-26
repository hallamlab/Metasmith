"""The `env` module: the sealed boundary for runtime selection and routing.

Nothing outside this package names a concrete runtime (DOCKER / APPTAINER /
MAMBA) or constructs a relay shell. Callers hold an `Environment` and a
`Runtime` selector; the per-runtime details -- how a tool is provisioned,
wrapped, invoked, bridged, and handed the host's GPUs -- live here.
"""

from .environment import ContainerDef, Environment, Runtime
from ._shell import Shell

# The relay client is owned by the env module; it is re-exported here so the
# one historical public re-export (python_api) can redirect through env
# rather than reaching into coms directly.
from ..coms.via_file_watcher import RemoteShell

__all__ = ["ContainerDef", "Environment", "Runtime", "Shell", "RemoteShell"]
