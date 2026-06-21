"""The `env` module: the sealed boundary for runtime selection and routing.

Nothing outside this package should name a concrete runtime (DOCKER /
APPTAINER / MAMBA) or construct a relay shell. Callers hold an
`Environment` and a `Runtime` selector; the per-runtime details live here.

Back-compat aliases (`Container`, `ContainerRuntime`) are re-exported for the
duration of the carve and will be removed once all call sites migrate to the
`Environment`/`Runtime` names.
"""

from .environment import Environment, Runtime

# Transitional aliases — call sites are migrating from the old names.
Container = Environment
ContainerRuntime = Runtime

__all__ = ["Environment", "Runtime", "Container", "ContainerRuntime"]
