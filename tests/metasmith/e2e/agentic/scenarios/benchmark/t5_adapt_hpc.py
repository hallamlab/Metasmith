from __future__ import annotations

from dataclasses import dataclass

from ._base import BenchmarkScenario

#: RECONCILE-ME: HPC identity is still open (sockeye?) — condition_matrix.md § G.
TARGET_HOST = "sockeye"


@dataclass
class AdaptHpcScenario(BenchmarkScenario):
    name: str = "t5_adapt_hpc"
    timeout_s: float = 3600.0   # micro compute; headroom for SLURM queue waits
    target_host: str = TARGET_HOST
