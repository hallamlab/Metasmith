"""What the executing host will actually schedule, and what to do about it.

A step that asks for more cpus or memory than its executor can give is never
scheduled. On a scheduler that means it queues against a bigger node; on the
local executor it means nextflow refuses it at submit, `errorStrategy = ignore`
swallows the refusal, and the run reports completed having produced nothing.
The retry ladder doubles the request on each attempt, so no retry recovers.

This is the launch-time check that says so first, next to the GPU one.
"""

from __future__ import annotations

import os
import re

from ..logging import Log


DISABLE_ENV = "METASMITH_SKIP_RESOURCE_CHECK"

_BLOCK = re.compile(r"executor\s*\{([^{}]*)\}")
_CPUS = re.compile(r"cpus\s*=\s*(\d+)")
_MEM_GB = re.compile(r"memory\s*=\s*'\s*([\d.]+)\s*GB\s*'")
_WITH_NAME = re.compile(r"withName:\s*'([^']+)'\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}")
_REQ_CPUS = re.compile(r"cpus\s*=?\s*(\d+)")
_REQ_MEM_GB = re.compile(r"'\s*([\d.]+)\s*GB\s*'")


class ResourceCeilingError(Exception):
    pass


def parse_ceiling(config_text: str) -> "tuple[int|None, float|None]":
    """The cpus and GB the last numeric `executor {}` block in `config_text` allows.

    The last one, because a nextflow config resolves a repeated assignment to
    whichever came last -- which is how the detected block and an explicit
    `params.executor` override the preset's static guess. Blocks whose values
    are `params.executor.cpus` and the like are skipped: they read correctly at
    runtime but are evaluated before `-params-file` is merged, so they never
    carry a number.
    """
    cpus = mem = None
    for body in _BLOCK.findall(config_text):
        m = _CPUS.search(body)
        if m: cpus = int(m.group(1))
        m = _MEM_GB.search(body)
        if m: mem = float(m.group(1))
    return cpus, mem


def parse_requests(resources_text: str) -> dict[str, "tuple[int|None, float|None]"]:
    """Per-process cpus and GB, from the `workflow.resources.nf` a stage wrote."""
    out: dict[str, tuple[int | None, float | None]] = {}
    for name, body in _WITH_NAME.findall(resources_text):
        cpus_m = _REQ_CPUS.search(body)
        mem_m = _REQ_MEM_GB.search(body)
        out[name] = (
            int(cpus_m.group(1)) if cpus_m else None,
            float(mem_m.group(1)) if mem_m else None,
        )
    return out


def find_breaches(
    requests: dict[str, "tuple[int|None, float|None]"],
    ceiling_cpus: "int|None", ceiling_gb: "float|None",
) -> list[str]:
    breaches = []
    for name, (cpus, gb) in sorted(requests.items()):
        over = []
        if ceiling_cpus is not None and cpus is not None and cpus > ceiling_cpus:
            over.append(f"{cpus} cpus > {ceiling_cpus}")
        if ceiling_gb is not None and gb is not None and gb > ceiling_gb:
            over.append(f"{gb:g} GB > {ceiling_gb:g} GB")
        if over:
            breaches.append(f"{name}: {' and '.join(over)}")
    return breaches


def refusal_message(breaches: list[str], ceiling_cpus, ceiling_gb) -> str:
    return (
        f"[{len(breaches)}] step(s) ask for more than this host will schedule"
        f" ({ceiling_cpus} cpus, {ceiling_gb:g} GB), so nextflow would refuse"
        f" each at submit and the run would report completed with their"
        f" products missing:\n  " + "\n  ".join(breaches) +
        f"\n\nRun it somewhere bigger, raise the ceiling with"
        f" `params.executor` (`executor: {{cpus: N, memory: 'M GB'}}` in the"
        f" run's params), lower the steps' requests with a resource override,"
        f" or set {DISABLE_ENV}=1 to launch anyway."
    )


def cap_lines(
    requests: dict[str, "tuple[int|None, float|None]"], ceiling_gb: "float|None",
) -> list[str]:
    """Config that stops the retry ladder doubling a request past the ceiling.

    Nextflow retries with `(2**(task.attempt-1)) * memory`, so the second
    attempt of a step that only just fits is unschedulable for the same reason
    the first attempt of an oversized one is -- and every attempt after it.
    """
    if ceiling_gb is None: return []
    TAB = "\t"
    lines = ["", "process {"]
    for name, (_cpus, gb) in sorted(requests.items()):
        if gb is None: continue
        lines += [
            TAB + f"withName: '{name}' " + "{",
            TAB + TAB + (
                f"memory = {{ def want = (2**(task.attempt-1)) * ('{gb:g} GB'"
                f" as MemoryUnit); def cap = ('{ceiling_gb:g} GB' as"
                f" MemoryUnit); want > cap ? cap : want }}"
            ),
            TAB + "}",
        ]
    lines += ["}", ""]
    return lines if len(lines) > 3 else []


def check_launch(
    config_text: str, resources_text: str, runs_on_this_host: bool,
) -> list[str]:
    """Raise if this host cannot schedule the plan; return the config to append.

    Only the executors that run tasks on the driver's own host earn a refusal.
    A scheduler queues an over-request against a node that fits, and refusing
    there would stop work that would have run.
    """
    if not runs_on_this_host:
        return []
    requests = parse_requests(resources_text)
    if not requests:
        return []
    ceiling_cpus, ceiling_gb = parse_ceiling(config_text)
    if ceiling_cpus is None and ceiling_gb is None:
        Log.Warn(
            "could not tell what this host will schedule, so no step's request"
            " was checked against it; a step that does not fit will be refused"
            " by nextflow at submit instead"
        )
        return []
    breaches = find_breaches(requests, ceiling_cpus, ceiling_gb)
    if breaches and os.environ.get(DISABLE_ENV, "").strip() not in {"", "0", "false", "no"}:
        Log.Warn(
            f"[{len(breaches)}] step(s) do not fit this host and"
            f" {DISABLE_ENV} is set, so launching anyway:\n  "
            + "\n  ".join(breaches)
        )
        breaches = []
    if breaches:
        raise ResourceCeilingError(
            refusal_message(breaches, ceiling_cpus, ceiling_gb)
        )
    return cap_lines(requests, ceiling_gb)
