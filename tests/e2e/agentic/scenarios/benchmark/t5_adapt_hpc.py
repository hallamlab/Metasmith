"""t5 — adapt → HPC (SLURM): run = confirm.

The pipeline already runs locally; the agent adapts it to run under the HPC
scheduler (apptainer + SLURM) and runs it there. A successful scheduler run to
the final clusterProfiler artifact IS the oracle, so the verifier is the
standard final-artifact + trace check.

HPC prep — agent originates on micb0, deploys over SSH, compute nodes have no
internet so images + DBs must be pre-staged on the login node — is the team-fill
hook ``prefetch_images_remote`` in ``_pipeline``. Locally the scenario renders
and provisions its start-state exactly like t3.
"""
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
