"""Declarative resource asks: what a step wants, in units a scheduler understands.

`Size` and `Duration` render themselves as Nextflow directives; `Resources`
collects them. The GPU pair is deliberately split across two declarations that
never meet in one file: `Gpus` (plus `Resources.gpu_memory`) is what a
*transform* may say -- a toggle and a total VRAM figure, the only units a tool
honestly knows -- while `Gpu` is what a *run* says about the host's devices.
Device count and device type are facts about a cluster, so they live on the run
side and a transform cannot name them.

Nothing here imports from elsewhere in metasmith, which is why it is the floor
of this package.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum


@dataclass
class Size:
    value_gb: float
    strict: bool=False

    def __str__(self) -> str:
        return self.AsNextflowFormat()

    @classmethod
    def TB(cls, val: float):
        return cls(value_gb=val*1024)

    @classmethod
    def GB(cls, val: float, strict: bool=False):
        return cls(value_gb=val)

    @classmethod
    def MB(cls, val: float, strict: bool=False):
        return cls(value_gb=val/1024)

    @classmethod
    def KB(cls, val: float, strict: bool=False):
        return cls(value_gb=val/(1024**2))
    
    def SetStrict(self):
        self.strict=True
        return self

    def AsNextflowFormat(self):
        return f"'{self.value_gb:0.2f} GB'"

class Duration:
    def __init__(self, days: float=0, seconds: float=0, microseconds: float=0,
                milliseconds: float=0, minutes: float=0, hours: float=0, weeks: float=0) -> None:
        self._delta = timedelta(
            days=days, seconds=seconds, microseconds=microseconds,
            milliseconds=milliseconds, minutes=minutes, hours=hours, weeks=weeks
        )
        self.strict=False
    
    def __str__(self) -> str:
        return self.AsNextflowFormat()

    def SetStrict(self):
        self.strict=True
        return self

    def AsNextflowFormat(self):
        delta = self._delta
        total_seconds = delta.total_seconds()
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        microseconds = delta.microseconds
        sd = f"{days}day{'s' if days!=1 else ''}"
        sh = f"{hours}hours"
        sm = f"{minutes}minutes"
        ss = f"{seconds}seconds"
        s = [x for x, v in zip([sd, sh, sm, ss], [days, hours, minutes, seconds]) if v>0]
        if len(s) == 0: s = [ss]
        return f"'{' '.join(s)}'"

class Gpus(Enum):
    # A pure toggle: whether the transform's tool needs a GPU, and how badly.
    # Deliberately carries no count and no device type -- how many devices a
    # given VRAM ask resolves to, and what a device is called, are facts about
    # the *host*, not the tool. Those live on `Gpu` (the run-side declaration).
    NONE = "none"
    OPTIONAL = "optional"
    REQUIRED = "required"

# Label attached at stage time to every process whose transform declared a GPU.
# Follows the existing `label 'x<name>x'` convention (see `xlocalx` in slurm.nf).
GPU_LABEL = "gpu"

@dataclass
class Gpu:
    """What a GPU *is* on the target host — the run-side half of the contract.

    Declared once per run via `Agent.RunWorkflow(gpus=...)`. This is the only
    place device vocabulary appears: per-device VRAM, the site's device/gres
    type token, how many devices a node has, and the scheduler flag shape used
    to ask for them. A transform never names any of these.

    `flag` is the request syntax including its separator, so the count appends
    directly: `--gpus-per-node=` -> `--gpus-per-node=2`, `--gres=gpu:` ->
    `--gres=gpu:2` (or `--gres=gpu:a100:2` when `type` is set).
    """
    memory: Size|None = None
    type: str|None = None
    count: int|None = None
    flag: str = "--gpus-per-node="
    # Scheduler flags a GPU step needs beyond the device count -- typically the
    # GPU partition, since a site's default partition has no cards. These go on
    # GPU steps only, which is what distinguishes them from
    # `params.process.clusterOptionsExtra` (every step). Sockeye needs
    # ["--partition=gpu"].
    extra: list[str] = field(default_factory=list)

    def DevicesFor(self, required: Size|None) -> int:
        # How many of *this* device it takes to total `required` VRAM. No ask
        # (or no declared per-device memory to divide by) means one device --
        # the transform said it wants a GPU without saying how much.
        if required is None or self.memory is None: return 1
        if self.memory.value_gb <= 0: return 1
        return max(1, math.ceil(required.value_gb / self.memory.value_gb))

    def MakeRequestFlag(self, devices: int) -> str:
        req = f"{self.flag}{self.type}:{devices}" if self.type else f"{self.flag}{devices}"
        return " ".join([req, *self.extra])

@dataclass
class Resources:
    cpus: int|None = None
    memory: Size|None = None
    duration: Duration|None = None
    # GPU need. `gpus` is the toggle; `gpu_memory` is the TOTAL VRAM the tool
    # needs, which is the unit a tool actually cares about. Unlike the three
    # fields above, `gpu_memory` is NOT a Nextflow directive -- Nextflow has no
    # VRAM concept -- so AsNextflowFormat never renders it. It is metasmith-side
    # input to the run-time device-count computation and to the protocol.
    gpus: Gpus = Gpus.NONE
    gpu_memory: Size|None = None

    @property
    def wants_gpu(self) -> bool:
        return self.gpus is not Gpus.NONE

    def AsNextflowFormat(self, is_config=False):
        def _parse_res(r:int|Duration|Size|None, var: str, field: str, norm: str, strict: str=""):
            if r is None: return None
            rval = str(r)
            if not isinstance(r, int):
                is_strict = r.strict
            else:
                is_strict = False
            if is_strict:
                val = strict.replace(var, rval)
            else:
                val = norm.replace(var, rval)
            joiner = " = " if is_config else " " # why is nextflow inconsistent like this??
            return f"{field}{joiner}{val}"
        # return [x for x in [
        #     _parse_res(self.cpus, "<x>", "cpus", "<x>"),
        #     _parse_res(
        #         self.memory, "<x>", "memory",
        #         "{"+f" task.attempt==1? <x> : 2*(<x> as MemoryUnit) "+"}",
        #         "<x>",
        #     ),
        #     _parse_res(
        #         self.duration, "<x>", "time",
        #         "{"+f" task.attempt==1? <x> : 2*(<x> as Duration) "+"}",
        #         "<x>",
        #     ),
        # ] if x is not None]
        return [x for x in [
            _parse_res(self.cpus, "<x>", "cpus", "<x>"),
            _parse_res(
                self.memory, "<x>", "memory",
                "{"+f" (2**(task.attempt-1)) * (<x> as MemoryUnit) "+"}",
                "<x>",
            ),
            _parse_res(
                self.duration, "<x>", "time",
                "{"+f" (2**(task.attempt-1)) * (<x> as Duration) "+"}",
                "<x>",
            ),
        ] if x is not None]
