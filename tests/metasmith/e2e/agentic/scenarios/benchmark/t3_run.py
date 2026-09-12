from __future__ import annotations

from dataclasses import dataclass

from ._base import BenchmarkScenario


@dataclass
class RunScenario(BenchmarkScenario):
    name: str = "t3_run"
    timeout_s: float = 900.0
