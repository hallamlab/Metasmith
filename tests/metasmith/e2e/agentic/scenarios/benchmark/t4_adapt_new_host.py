from __future__ import annotations

from dataclasses import dataclass

from ._base import BenchmarkScenario

#: RECONCILE-ME: the target host for this port (see condition_matrix.md § E).
TARGET_HOST = "chamois"


@dataclass
class AdaptNewHostScenario(BenchmarkScenario):
    name: str = "t4_adapt_new_host"
    timeout_s: float = 1800.0   # micro compute; headroom for remote transfer/startup
    target_host: str = TARGET_HOST
