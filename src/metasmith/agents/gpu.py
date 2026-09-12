# The host's `CUDA_VISIBLE_DEVICES` must NOT be forwarded verbatim. Under a
# scheduler that constrains devices by cgroup (SLURM with ConstrainDevices) the
# container sees only the allocated device, renumbered from 0, while the host's
# variable names the index on the node -- so anything but GPU 0 asks for a device
# outside what it can see and `cuInit` fails indistinguishably from a container
# with no GPU support. Unset on the host, naive forwarding sends the empty
# string, which CUDA reads as "no devices" rather than "unset" -- same symptom.
# So it is left unset, except for a MIG slice's handle (`MIG-<uuid>`, not an
# index), which cannot be re-derived from enumeration.
from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from ..coms.terminals import LiveShell
from ..constants import AgentPaths
from ..logging import Log
from ..models.libraries import GPU_LABEL, Gpu, Gpus, Size

class GpuRequirementError(Exception):
    pass
_GPU_BEFORE_SCRIPT = (
    'case "${CUDA_VISIBLE_DEVICES:-}" in '
    '*MIG-*) export APPTAINERENV_CUDA_VISIBLE_DEVICES="$CUDA_VISIBLE_DEVICES"; '
    'export SINGULARITYENV_CUDA_VISIBLE_DEVICES="$CUDA_VISIBLE_DEVICES";; '
    '*) unset APPTAINERENV_CUDA_VISIBLE_DEVICES SINGULARITYENV_CUDA_VISIBLE_DEVICES;; '
    'esac'
)

def _read_gpu_manifest(shell: LiveShell, workspace: Path) -> dict[str, dict]:
    path = workspace/AgentPaths.GPU_MANIFEST
    res = shell.Exec(f'[ -e "{path}" ] && cat "{path}"', history=True, quiet=True)
    text = "\n".join(res.out)
    if "{" not in text: return {}
    try:
        text = text[text.index("{"):text.rindex("}")+1]
        return json.loads(text).get("steps", {})
    except (json.JSONDecodeError, ValueError) as e:
        raise GpuRequirementError(
            f"GPU manifest at [{path}] exists but could not be parsed ({e});"
            f" cannot verify GPU requirements. Re-stage the workflow."
        ) from e

def _plan_gpu_requests(
        manifest: dict[str, dict],
        device: Gpu|None,
        detect: "Callable[[], str]|None" = None,
    ) -> dict[str, int]:
    if not manifest: return {}
    required = [v for v in manifest.values() if v.get("gpus") == Gpus.REQUIRED.value]
    if device is None:
        if not required: return {}
        offenders = ", ".join(sorted(f"{v['transform']} (step {v['step']})" for v in required))
        detail = ""
        if detect is not None:
            found = detect()
            if found:
                detail = (
                    f" a GPU does appear to be present on the target"
                    f" [{found}] -- declare it with RunWorkflow(gpus=Gpu(memory=Size.GB(...)))."
                )
        raise GpuRequirementError(
            f"workflow requires a GPU but none was declared for this run;"
            f" offending transforms: {offenders}.{detail}"
        )

    planned: dict[str, int] = {}
    over: list[str] = []
    for process, v in sorted(manifest.items()):
        ask = v.get("gpu_memory_gb")
        n = device.DevicesFor(None if ask is None else Size.GB(ask))
        if device.count is not None and n > device.count:
            over.append(
                f"{v['transform']} (step {v['step']}) needs {ask} GB"
                f" -> {n} device(s), but only {device.count} are declared"
            )
            continue
        if n > 1:
            Log.Warn(
                f"GPU request for [{v['transform']}] (step {v['step']}) spans {n} devices"
                f" ({ask} GB over {device.memory.value_gb:g} GB per device) -- the tool must"
                f" be able to shard across cards, or this will fail at run time"
            )
        planned[process] = n
    if over:
        raise GpuRequirementError(
            "declared GPU cannot satisfy the workflow: " + "; ".join(over)
        )
    return planned

def _render_gpu_config(planned: dict[str, int], device: Gpu|None, scheduler: bool) -> list[str]:
    if not planned: return []
    TAB = "\t"
    lines = ["", "process {", TAB+f"withLabel: 'x{GPU_LABEL}x' "+"{",
             TAB+TAB+f"beforeScript = '{_GPU_BEFORE_SCRIPT}'", TAB+"}"]
    if scheduler and device is not None:
        for process, n in planned.items():
            base = (
                "(params.process.clusterOptions ?: "
                '"--nodes=1 --ntasks=1 --account=${params.slurmGpuAccount ?: params.slurmAccount}")'
            )
            extra = '(params.process.clusterOptionsExtra ? " ${params.process.clusterOptionsExtra}" : "")'
            lines += [
                TAB+f"withName: '{process}' "+"{",
                TAB+TAB+f'clusterOptions = {base} + " {device.MakeRequestFlag(n)}" + {extra}',
                TAB+"}",
            ]
    lines += ["}", ""]
    return lines
