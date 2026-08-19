from __future__ import annotations

from dataclasses import dataclass, field

from ._base import BenchmarkScenario
from ..base import PromptContext
from ._pipeline import FINAL_ARTIFACT_GLOB

#: TODO hook: nf-core/bacass provenance sub-study for the Nextflow arms + A10.
NFCORE_TODO = "stage_nfcore_bacass"

ABRICATE_REPORT_REL = "workspace/results/abricate.tsv"


@dataclass
class AddToolScenario(BenchmarkScenario):
    name: str = "t6_adapt_add_tool"
    timeout_s: float = 900.0
    expected_artifact_globs: list[str] = field(
        default_factory=lambda: [FINAL_ARTIFACT_GLOB, ABRICATE_REPORT_REL]
    )

    def data_lines(self, ctx: PromptContext) -> list[str]:
        sb = str(ctx.sandbox)
        return [
            "added tool: `abricate` is pre-installed (AMR/virulence screen; "
            "consumes the assembled contigs; DBs bundled in its container)",
            "abricate report: write the abricate screen output to "
            f"{sb}/{ABRICATE_REPORT_REL}",
        ]
