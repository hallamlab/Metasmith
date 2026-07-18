"""Common base for the pipeline benchmark scenarios (t2..t7).

Each concrete scenario is a small dataclass that overrides ``name`` (and,
where needed, ``timeout_s`` / extra fixtures / extra DATA lines). The three
Scenario-protocol methods are shared here:

  * ``build_prompt``   — ``compose_benchmark_prompt(shared_goal_block(...), arm)``
  * ``setup_fixtures`` — dispatch the arm's start-state via ``provision_for``
  * ``verify``         — ``standard_verify`` (metasmith trace auto-gated by arm)

t1 (install) does NOT use this base — its goal/oracle are per-tool, not the
pipeline goal — so it is a standalone dataclass.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ...harness.loop import LoopResult
from ...harness.sandbox import SandboxLayout
from ...install_mock.verify_local_artifacts import InstallContext
from ..arms import Arm, DEFAULT_ARM
from ..base import PromptContext, VerifyContext, standard_verify
from ._pipeline import (
    EXPECTED_TRACE,
    FINAL_ARTIFACT_GLOB,
    compose_benchmark_prompt,
    provision_for,
    shared_goal_block,
)


@dataclass
class BenchmarkScenario:
    """Base pipeline scenario. Subclasses set ``name`` (+ optionals)."""

    name: str = "benchmark"
    tutorial_path: str = ""     # benchmark scenarios are prompt-driven, no tutorial
    expected_artifact_globs: list[str] = field(
        default_factory=lambda: [FINAL_ARTIFACT_GLOB]
    )
    expected_trace: tuple[str, str] | None = EXPECTED_TRACE
    timeout_s: float = 3600.0
    pre_install_metasmith: bool = True   # metasmith is the harness control plane

    # --- hooks a subclass may override -------------------------------------

    def data_lines(self, ctx: PromptContext) -> list[str]:
        """Extra DATA bullet lines (arm-independent — depend only on the test)."""
        return []

    def extra_fixtures(self, layout: SandboxLayout, ctx: InstallContext,
                       arm: Arm) -> None:
        """Scenario-specific fixtures staged in addition to the arm start-state."""
        return None

    # --- Scenario protocol -------------------------------------------------

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
        )
