"""Turning declared GPU requirements into a Nextflow config, and refusing early.

Two halves that belong together: reading what each step asked for out of the
staged manifest, and rendering the `withName:` selectors that ask the scheduler
for it. In between sits the preflight -- a workflow that declares a GPU and an
agent with no device is a failure worth having before submission, not after.

`_GPU_BEFORE_SCRIPT` is the awkward part. Apptainer runs tool containers with
`--cleanenv`, which strips `CUDA_VISIBLE_DEVICES` and so hides a partial
allocation (or a MIG slice, whose handle is a `MIG-<uuid>` rather than an index)
from the tool. Re-exporting under the `APPTAINERENV_`/`SINGULARITYENV_` prefixes
is the way back in, and a harmless no-op under Docker and mamba/native.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from ..coms.terminals import LiveShell
from ..constants import AgentPaths
from ..logging import Log
from ..models.libraries import GPU_LABEL, Gpu, Gpus, Size

class GpuRequirementError(Exception):
    """A staged workflow's GPU requirements cannot be met by this run's declaration."""

# Emitted into workflow.config.nf whenever a run declares a GPU. Apptainer runs
# tool containers with --cleanenv (see env/environment.py), which strips
# CUDA_VISIBLE_DEVICES and so hides a partial allocation -- or a MIG slice, whose
# handle is a MIG-<uuid> rather than an index -- from the tool. Re-exporting it
# under the APPTAINERENV_/SINGULARITYENV_ prefixes is the way back in, and is a
# harmless no-op under Docker and mamba/native. Single-quoted in the emitted
# Groovy so `$` survives to the shell rather than being interpolated.
_GPU_BEFORE_SCRIPT = (
    'export APPTAINERENV_CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-}"; '
    'export SINGULARITYENV_CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-}"'
)

def _read_gpu_manifest(shell: LiveShell, workspace: Path) -> dict[str, dict]:
    # Per-step GPU asks recorded at stage time. Absent for workspaces staged by
    # an older metasmith, which is indistinguishable from "no step wants a GPU"
    # and is treated as such.
    path = workspace/AgentPaths.GPU_MANIFEST
    res = shell.Exec(f'[ -e "{path}" ] && cat "{path}"', history=True, quiet=True)
    text = "\n".join(res.out)
    if "{" not in text: return {}
    try:
        text = text[text.index("{"):text.rindex("}")+1]
        return json.loads(text).get("steps", {})
    except (json.JSONDecodeError, ValueError) as e:
        # A file that exists but does not parse means we cannot tell whether a
        # step requires a GPU. Proceeding would silently skip the very check
        # this feature exists to perform, so refuse instead.
        raise GpuRequirementError(
            f"GPU manifest at [{path}] exists but could not be parsed ({e});"
            f" cannot verify GPU requirements. Re-stage the workflow."
        ) from e

def _plan_gpu_requests(
        manifest: dict[str, dict],
        device: Gpu|None,
        detect: "Callable[[], str]|None" = None,
    ) -> dict[str, int]:
    """Reconcile the staged per-step GPU asks against this run's declaration.

    Returns process-name -> device count for the steps that should be submitted
    with a GPU request. Raises GpuRequirementError when a REQUIRED step cannot
    be satisfied; OPTIONAL steps never fail here -- they render without GPU
    flags and let the protocol's own detection take the CPU branch.
    """
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
            # Splitting a VRAM ask across cards only works for tools that can
            # shard; most cannot, and they fail deep inside CUDA rather than at
            # submission. Loud, per-step, and named.
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
    # The shared label block carries only what does not vary per step; the
    # per-step withName blocks carry the device count, which is the one thing
    # that could not be known at stage time. withName outranks withLabel in
    # nextflow, and this file is loaded after workflow.resources.nf, so a
    # resource_overrides entry for the same step still wins per-directive.
    if not planned: return []
    TAB = "\t"
    lines = ["", "process {", TAB+f"withLabel: 'x{GPU_LABEL}x' "+"{",
             TAB+TAB+f"beforeScript = '{_GPU_BEFORE_SCRIPT}'", TAB+"}"]
    if scheduler and device is not None:
        for process, n in planned.items():
            # clusterOptions is a scalar directive: setting it here REPLACES the
            # base string, so the base flags have to be restated or every job is
            # rejected by SLURM for a missing account.
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
