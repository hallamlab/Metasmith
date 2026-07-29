"""t2 — pipeline (create): define + dry-validate the pipeline end-to-end.

``Done`` = the pipeline is defined and DRY-VALIDATES end-to-end (no full run).
The goal text (from ``_pipeline._GOALS['t2_pipeline']``) tells every arm to stop
at a dry-run / plan validation that shows every stage wires up from the reads to
the final clusterProfiler artifact.

Because there is no full execution, the final PNG is NOT produced; the oracle is
the agent's own ``checkpoint done`` (self-report) plus the dry-validation
artifact the agent is asked to write. We therefore relax the artifact glob to the
validation marker rather than the final PNG, and skip the metasmith trace check
(nothing ran, so there is no results.xgdb / lineage yet).
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ._base import BenchmarkScenario
from ..base import GoldenCheck, PromptContext


_VALIDATION_REL = "workspace/results/DRYRUN_OK.txt"


@dataclass
class PipelineScenario(BenchmarkScenario):
    name: str = "t2_pipeline"
    timeout_s: float = 600.0   # dry-validate: no run, checker is a no-op
    # t2 is a dry-validate: the agent submits after its plan/dry-run passes and
    # writes the marker; the checker does NOT execute the pipeline.
    checker_action: str = "validate"
    # A dry-validate leaves no run artifacts and no lineage store — check the
    # validation marker the agent writes, not the final PNG, and skip the trace.
    expected_artifact_globs: list[str] = field(
        default_factory=lambda: [_VALIDATION_REL]
    )
    expected_trace: tuple[str, str] | None = None
    # Dry-validate produces no final PNG/TSV, so the golden content check does not
    # apply — the marker + self-report are the oracle.
    golden_check: GoldenCheck | None = None

    def data_lines(self, ctx: PromptContext) -> list[str]:
        sb = str(ctx.sandbox)
        return [
            "dry-validation marker: when the pipeline is defined and its dry-run "
            f"passes, write a one-line file {sb}/{_VALIDATION_REL}",
        ]
