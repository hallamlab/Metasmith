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
        self.unlimited=False

    @classmethod
    def Unlimited(cls):
        d = cls()
        d.unlimited = True
        return d

    def __str__(self) -> str:
        return self.AsNextflowFormat()

    def SetStrict(self):
        assert not self.unlimited, "an unlimited duration cannot be strict: it can never time out"
        self.strict=True
        return self

    def AsNextflowFormat(self):
        if self.unlimited: return "null"
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
    NONE = "none"
    OPTIONAL = "optional"
    REQUIRED = "required"

GPU_LABEL = "gpu"

@dataclass
class Gpu:
    memory: Size|None = None
    type: str|None = None
    count: int|None = None
    flag: str = "--gpus-per-node="
    extra: list[str] = field(default_factory=list)

    def DevicesFor(self, required: Size|None) -> int:
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
            joiner = " = " if is_config else " "
            return f"{field}{joiner}{val}"
        return [x for x in [
            _parse_res(self.cpus, "<x>", "cpus", "<x>"),
            _parse_res(
                self.memory, "<x>", "memory",
                "{"+f" (2**(task.attempt-1)) * (<x> as MemoryUnit) "+"}",
                "<x>",
            ),
            # CAUTION the retry ladder doubles duration as well as memory, so a base of B reaches
            # B * 2^(attempts-1) on its last rung. A cluster that refuses over-long jobs AT SUBMIT
            # TIME then rejects those upper rungs outright -- and an ignored SUBMISSION failure
            # decrements nextflow's running-task counter with no matching increment, so the monitor
            # never sees the queue drain and the whole run wedges. Measured on fir, which caps every
            # job at 7.0 days regardless of partition: a 48 h base asked 192 h and 384 h on rungs 3
            # and 4, both refused, leaving runs at runningCount -6 and -7.
            # The ceiling therefore comes from params, not from a constant, because codegen runs in
            # the AGENT process and a driver-side setting would never reach it; `params` is readable
            # from a directive closure (the rendered config already uses params.process.tries).
            # The Elvis default keeps this behaviour-preserving wherever no ceiling is set.
            #
            # BOTH spellings are read, and that is not belt-and-braces -- either one alone is dead.
            # `RunWorkflow(params=<dict>)` passes every key through a parser that splits ANY key
            # containing an underscore into nested maps, so `{"process": {"max_duration": "24h"}}`
            # reaches the params file as `process: {max: {duration: 24h}}` and a closure reading
            # `params.process.max_duration` sees null. That is exactly how E3's vConTACT3 ladder
            # reached an 8-day rung with a 7-day ceiling set: the clamp was written but never armed,
            # sbatch refused the submission, and the ignored failure wedged the run. A params FILE
            # (`params=<Path>`) bypasses that parser, so the flat spelling is what a file delivers.
            # Keep both until the parser stops splitting underscores.
            _parse_res(
                self.duration, "<x>", "time",
                "<x>" if (self.duration is not None and self.duration.unlimited)
                else "{"+f" [(2**(task.attempt-1)) * (<x> as Duration), ((params.process?.max_duration ?: params.process?.max?.duration ?: '3650days') as Duration)].min() "+"}",
                "<x>",
            ),
        ] if x is not None]
