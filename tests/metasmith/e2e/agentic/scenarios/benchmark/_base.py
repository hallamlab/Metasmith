from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ...harness.loop import LoopResult
from ...harness.sandbox import SandboxLayout
from ...install_mock.verify_local_artifacts import InstallContext
from ..arms import Arm, DEFAULT_ARM
from ..base import GoldenCheck, PromptContext, VerifyContext, standard_verify
from ._pipeline import (
    EXPECTED_TRACE,
    FINAL_ARTIFACT_GLOB,
    FINAL_TABLE_GLOB,
    GOLDEN_MIN_PNG_BYTES,
    GOLDEN_MIN_TSV_ROWS,
    GOLDEN_TSV_REQUIRED_COLUMNS,
    compose_benchmark_prompt,
    provision_for,
    shared_goal_block,
)

_GOLDEN_CHECK = GoldenCheck(
    png_glob=FINAL_ARTIFACT_GLOB,
    table_glob=FINAL_TABLE_GLOB,
    min_png_bytes=GOLDEN_MIN_PNG_BYTES,
    required_columns=GOLDEN_TSV_REQUIRED_COLUMNS,
    min_rows=GOLDEN_MIN_TSV_ROWS,
)


@dataclass
class BenchmarkScenario:
    name: str = "benchmark"
    tutorial_path: str = ""
    expected_artifact_globs: list[str] = field(
        default_factory=lambda: [FINAL_ARTIFACT_GLOB]
    )
    expected_trace: tuple[str, str] | None = EXPECTED_TRACE
    golden_check: GoldenCheck | None = field(default_factory=lambda: _GOLDEN_CHECK)
    timeout_s: float = 900.0
    pre_install_metasmith: bool = True
    max_tokens: int | None = None

    checker_action: str = "run"
    checker_resource_overrides: dict | None = field(
        default_factory=lambda: {"all": {"cpus": 8}}
    )


    def data_lines(self, ctx: PromptContext) -> list[str]:
        return []

    def extra_fixtures(self, layout: SandboxLayout, ctx: InstallContext,
                       arm: Arm) -> None:
        return None


    def build_prompt(self, ctx: PromptContext) -> str:
        shared = shared_goal_block(
            self.name,
            sandbox=ctx.sandbox,
            done_key=self.name,
            data_lines=self.data_lines(ctx),
        )
        return compose_benchmark_prompt(shared, ctx.arm)

    def setup_fixtures(self, layout: SandboxLayout, ctx: InstallContext,
                       arm: Arm = DEFAULT_ARM) -> None:
        provision_for(arm)(layout, ctx)
        self.extra_fixtures(layout, ctx, arm)

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        return standard_verify(
            vctx, result,
            artifact_globs=self.expected_artifact_globs,
            expected_trace=self.expected_trace,
            golden_check=self.golden_check,
        )
