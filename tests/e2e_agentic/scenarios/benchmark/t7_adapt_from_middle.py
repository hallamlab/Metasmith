"""t7 — adapt → from halfway: resume from pre-computed assembly contigs.

Pre-computed assembly contigs are provided; the agent resumes the pipeline from
the assembly stage — reusing the given contigs (NOT re-running fastp/SPAdes) and
running the downstream stages (bakta → eggNOG-mapper → clusterProfiler).

``Done`` = a downstream artifact (the final clusterProfiler enrichment PNG) is
produced with the assembly REUSED, not recomputed. The reuse condition is a
process fact the token/wall-clock measurement captures (the run should not spend
time in fastp/SPAdes); the artifact oracle here is the standard final PNG plus a
trace check that still bottoms out at the reads (metasmith arms carry the reused
assembly's lineage forward).

Provenance variants (condition_matrix.md § D): toy is fully real; nf-core/bacass
is the ``stage_nfcore_bacass`` team-fill hook (see t6).
"""
from __future__ import annotations

from dataclasses import dataclass

from ._base import BenchmarkScenario
from ..arms import Arm
from ..base import PromptContext
from ...harness.sandbox import SandboxLayout
from ...install_mock.verify_local_artifacts import InstallContext
from ._pipeline import build_intermediate_contigs


_CONTIGS_REL = "workspace/precomputed/contigs.fasta"


@dataclass
class FromMiddleScenario(BenchmarkScenario):
    name: str = "t7_adapt_from_middle"
    timeout_s: float = 2700.0

    def data_lines(self, ctx: PromptContext) -> list[str]:
        sb = str(ctx.sandbox)
        return [
            "pre-computed assembly contigs (REUSE these; do NOT re-run "
            f"fastp/SPAdes): {sb}/{_CONTIGS_REL}",
        ]

    def extra_fixtures(self, layout: SandboxLayout, ctx: InstallContext,
                       arm: Arm) -> None:
        # Stage the pre-computed contigs the agent must resume from.
        build_intermediate_contigs(layout)
