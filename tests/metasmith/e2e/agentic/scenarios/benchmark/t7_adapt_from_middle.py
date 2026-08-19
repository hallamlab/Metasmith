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
    timeout_s: float = 900.0

    def data_lines(self, ctx: PromptContext) -> list[str]:
        sb = str(ctx.sandbox)
        return [
            "pre-computed assembly contigs (REUSE these; do NOT re-run "
            f"fastp/SPAdes): {sb}/{_CONTIGS_REL}",
        ]

    def extra_fixtures(self, layout: SandboxLayout, ctx: InstallContext,
                       arm: Arm) -> None:
        build_intermediate_contigs(layout)
