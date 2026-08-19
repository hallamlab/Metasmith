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
            _parse_res(
                self.duration, "<x>", "time",
                "<x>" if (self.duration is not None and self.duration.unlimited)
                else "{"+f" (2**(task.attempt-1)) * (<x> as Duration) "+"}",
                "<x>",
            ),
        ] if x is not None]
