"""Unit tests for the Arm abstraction + prompt-composition seam.

No agent, no sandbox — pure data/logic. Verifies the exact 10-arm
enumeration from the condition matrix (§ B), the metasmith discriminator,
the shared+preamble prompt seam, and that ``standard_verify`` runs the
metasmith-only trace check only for the metasmith arm.
"""
from __future__ import annotations

from pathlib import Path

from tests.e2e.agentic.scenarios.arms import (
    ARMS,
    ARM_BY_ID,
    DEFAULT_ARM,
    Arm,
)
from tests.e2e.agentic.scenarios.base import (
    VerifyContext,
    compose_prompt,
    standard_verify,
)
from tests.e2e.agentic.harness.loop import LoopOutcome, LoopResult


# ---------------------------------------------------------------------------
# enumeration
# ---------------------------------------------------------------------------


def test_exactly_ten_arms() -> None:
    assert len(ARMS) == 10
    assert [a.id for a in ARMS] == [f"A{i}" for i in range(1, 11)]


def test_arm_by_id_round_trip() -> None:
    assert set(ARM_BY_ID) == {a.id for a in ARMS}
    for a in ARMS:
        assert ARM_BY_ID[a.id] is a


def test_non_metasmith_arms_are_the_3x3_factorial() -> None:
    non_ms = [(a.env, a.orchestrator) for a in ARMS if not a.is_metasmith]
    assert len(non_ms) == 9
    envs = {"ad-hoc", "mamba", "container"}
    orchs = {"ad-hoc", "snakemake", "nextflow"}
    assert set(non_ms) == {(e, o) for e in envs for o in orchs}


def test_a10_is_full_metasmith() -> None:
    a10 = ARM_BY_ID["A10"]
    assert a10.env == "metasmith" and a10.orchestrator == "metasmith"
    assert a10.is_metasmith
    assert DEFAULT_ARM is a10


def test_only_a10_is_metasmith() -> None:
    assert [a.id for a in ARMS if a.is_metasmith] == ["A10"]


def test_matrix_arm_labels() -> None:
    """Spot-check ids against the condition matrix § B table."""
    expected = {
        "A1": ("ad-hoc", "ad-hoc"),
        "A2": ("ad-hoc", "snakemake"),
        "A3": ("ad-hoc", "nextflow"),
        "A4": ("mamba", "ad-hoc"),
        "A6": ("mamba", "nextflow"),
        "A9": ("container", "nextflow"),
        "A10": ("metasmith", "metasmith"),
    }
    for aid, (env, orch) in expected.items():
        assert (ARM_BY_ID[aid].env, ARM_BY_ID[aid].orchestrator) == (env, orch)


# ---------------------------------------------------------------------------
# compose_prompt seam
# ---------------------------------------------------------------------------


def test_compose_prompt_metasmith_is_byte_identical() -> None:
    shared = "GOAL\n\nDATA\n\nDONE\n"
    assert compose_prompt(shared, DEFAULT_ARM) == shared


def test_compose_prompt_prepends_non_empty_preamble() -> None:
    arm = Arm(id="AX", env="mamba", orchestrator="nextflow",
              preamble="Your env: mamba. Orchestrator: nextflow.")
    shared = "GOAL\n\nDATA\n\nDONE\n"
    out = compose_prompt(shared, arm)
    assert out.endswith(shared)
    assert out.startswith("Your env: mamba. Orchestrator: nextflow.")
    # shared block is preserved verbatim inside the composed prompt
    assert shared in out


# ---------------------------------------------------------------------------
# standard_verify gates the metasmith-only trace check by arm
# ---------------------------------------------------------------------------


def _done_result() -> LoopResult:
    return LoopResult(
        outcome=LoopOutcome.DONE,
        iterations=1,
        tokens_used=0,
        last_iter=None,
        terminal_control=None,
    )


def test_standard_verify_skips_trace_for_non_metasmith_arm(tmp_path: Path) -> None:
    sandbox = tmp_path / "sb"
    (sandbox / "out").mkdir(parents=True)
    (sandbox / "out" / "artifact.txt").write_text("x")
    non_ms = ARM_BY_ID["A7"]  # container / ad-hoc
    vctx = VerifyContext(
        sandbox=sandbox,
        agent_env={},
        metasmith_env_name="msm_env",
        installed_env_path=sandbox / "envs" / "msm_env",
        arm=non_ms,
    )
    # expected_trace is set, but a non-metasmith arm must NOT attempt the
    # `metasmith data trace` check (there is no results.xgdb / metasmith bin).
    fails = standard_verify(
        vctx, _done_result(),
        artifact_globs=["out/*.txt"],
        expected_trace=("ncbi::assembly_accession", "pangenome::heatmap"),
    )
    assert fails == []


def test_standard_verify_runs_trace_for_metasmith_arm(tmp_path: Path) -> None:
    sandbox = tmp_path / "sb"
    (sandbox / "out").mkdir(parents=True)
    (sandbox / "out" / "artifact.txt").write_text("x")
    vctx = VerifyContext(
        sandbox=sandbox,
        agent_env={},
        metasmith_env_name="msm_env",
        installed_env_path=sandbox / "envs" / "msm_env",
        arm=DEFAULT_ARM,  # metasmith
    )
    fails = standard_verify(
        vctx, _done_result(),
        artifact_globs=["out/*.txt"],
        expected_trace=("ncbi::assembly_accession", "pangenome::heatmap"),
    )
    # metasmith arm DOES run the trace check; here the result has no terminal
    # control, so _trace_failures reports the trace was skipped for lack of a
    # task_key — a message only reachable via the metasmith branch, proving it
    # fired (a non-metasmith arm would never emit any "trace" failure).
    assert any("trace" in f for f in fails)
