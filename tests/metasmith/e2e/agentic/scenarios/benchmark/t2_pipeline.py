from __future__ import annotations

from dataclasses import dataclass, field

from ._base import BenchmarkScenario
from ..base import GoldenCheck, PromptContext


_VALIDATION_REL = "workspace/results/DRYRUN_OK.txt"


@dataclass
class PipelineScenario(BenchmarkScenario):
    name: str = "t2_pipeline"
    timeout_s: float = 600.0
    checker_action: str = "validate"
    expected_artifact_globs: list[str] = field(
        default_factory=lambda: [_VALIDATION_REL]
    )
    expected_trace: tuple[str, str] | None = None
    golden_check: GoldenCheck | None = None

    def data_lines(self, ctx: PromptContext) -> list[str]:
        sb = str(ctx.sandbox)
        return [
            "dry-validation marker: when the pipeline is defined and its dry-run "
            f"passes, write a one-line file {sb}/{_VALIDATION_REL}",
        ]
