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

#: Golden content oracle shared by every pipeline scenario (t2..t7). t1 (install)
#: does not use this base, so it never runs the golden check.
_GOLDEN_CHECK = GoldenCheck(
    png_glob=FINAL_ARTIFACT_GLOB,
    table_glob=FINAL_TABLE_GLOB,
    min_png_bytes=GOLDEN_MIN_PNG_BYTES,
    required_columns=GOLDEN_TSV_REQUIRED_COLUMNS,
    min_rows=GOLDEN_MIN_TSV_ROWS,
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
    # Golden content oracle (T3). Default: check the produced PNG/TSV are real +
    # correctly-shaped. t2 (dry-validate, no run artifacts) overrides to None.
    golden_check: GoldenCheck | None = field(default_factory=lambda: _GOLDEN_CHECK)
    # Checker execution bound (metasmith `workflow run/wait/collect` or the
    # baseline entrypoint). The MICRO pipeline computes in ~1.5 min; 900 s leaves
    # generous headroom for metasmith plan-compile + 5 container starts + collect.
    # Remote scenarios (t4/t5) raise this for transfer/scheduler latency.
    timeout_s: float = 900.0
    pre_install_metasmith: bool = True   # metasmith is the harness control plane
    # Per-test cumulative token quota (billable in+cached+out+cache_creation).
    # None → run_cell falls back to the global default. Subclasses override with
    # a pilot-discovered value; a cell that reaches this quota stops as a DNF
    # (outcome=over_budget). See run_cell._effective_max_tokens.
    max_tokens: int | None = None

    # --- submit/checker knobs (see harness/checker.run_checker) --------------
    # What the non-agentic checker does with a SUBMITTED submission:
    #   "run"      — execute the pipeline (metasmith workflow run/wait/collect,
    #                or the baseline entrypoint) and materialize the artifact.
    #   "validate" — no execution (t2: the agent's dry-validate wrote the marker).
    checker_action: str = "run"
    # Per-step resource overrides applied on the metasmith `workflow run`. The
    # key ``"all"`` maps to a ``withName: '.*'`` selector (every process), so
    # this forces threads on the whole pipeline regardless of whether each
    # transform's own Resources(cpus=…) directive takes effect — the belt to the
    # transform-declaration suspenders (see plan T3; the pilot saw tasks pinned
    # to 1 CPU → SPAdes single-threaded). None → rely on the declarations only.
    # t2 (checker_action="validate") never executes, so this is a no-op there.
    checker_resource_overrides: dict | None = field(
        default_factory=lambda: {"all": {"cpus": 8}}
    )

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
            golden_check=self.golden_check,
        )
