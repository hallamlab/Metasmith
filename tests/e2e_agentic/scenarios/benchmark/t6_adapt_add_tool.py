"""t6 — adapt + new tool: splice `abricate` into the running pipeline.

The E. coli pipeline already runs; the agent splices the pre-installed
``abricate`` AMR/virulence screen (it consumes the assembled contigs) into the
pipeline, then runs so BOTH the abricate report AND the final clusterProfiler
enrichment artifact are produced. The test measures pipeline-rewiring, not
install — abricate is pre-installed on every channel beforehand (its sif is in
the staged image list; ``build_env_spec(container)`` includes it).

``Done`` = pipeline runs with the added tool. The oracle requires the final PNG
(shared) AND the abricate report under ``workspace/results/``.

Provenance variants (condition_matrix.md § D): the default toy provenance is
fully real. The nf-core/bacass variant (NF arms + A10) is a documented TODO hook
(``stage_nfcore_bacass`` in ``_pipeline``) — see ``NFCORE_TODO`` below.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ._base import BenchmarkScenario
from ..base import PromptContext
from ._pipeline import FINAL_ARTIFACT_GLOB

#: TODO hook: nf-core/bacass provenance sub-study for the Nextflow arms + A10.
#: Wire ``_pipeline.stage_nfcore_bacass`` into ``extra_fixtures`` when the
#: provenance == 'nf-core'. Toy provenance is the default and fully provisioned.
NFCORE_TODO = "stage_nfcore_bacass"

#: The added tool's report, arm-independent canonical location.
ABRICATE_REPORT_REL = "workspace/results/abricate.tsv"


@dataclass
class AddToolScenario(BenchmarkScenario):
    name: str = "t6_adapt_add_tool"
    timeout_s: float = 3600.0
    # Both the final enrichment PNG AND the abricate report must appear.
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
