"""t4 — adapt → new host (chamois): run = confirm.

The pipeline already runs on the origin host; the agent adapts it to the new
host (chamois: docker-denied → apptainer use-sandbox) and runs it there. A
successful run to the final clusterProfiler artifact IS the oracle (no separate
scored step), so the verifier is the standard final-artifact + trace check.

The remote host prep (mirroring sifs + reference DBs onto chamois) is the
team-fill hook ``prefetch_images_remote`` in ``_pipeline``; locally the scenario
renders and provisions its start-state exactly like t3.
"""
from __future__ import annotations

from dataclasses import dataclass

from ._base import BenchmarkScenario

#: RECONCILE-ME: the target host for this port (see condition_matrix.md § E).
TARGET_HOST = "chamois"


@dataclass
class AdaptNewHostScenario(BenchmarkScenario):
    name: str = "t4_adapt_new_host"
    timeout_s: float = 5400.0
    target_host: str = TARGET_HOST
