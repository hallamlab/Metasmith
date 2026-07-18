"""Unit tests for the 7 benchmark scenarios (no agent, no sandbox).

Verifies every benchmark scenario imports cleanly and renders a prompt for the
two reference arms A10 (metasmith) and A7 (container/ad-hoc) without error, and
that the study's prompt-symmetry invariant holds: the shared GOAL/DATA/DONE block
is byte-identical across arms — only ``arm.preamble`` differs.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e_agentic.scenarios.arms import ARM_BY_ID, DEFAULT_ARM
from tests.e2e_agentic.scenarios.base import PromptContext
from tests.e2e_agentic.scenarios.benchmark import BENCHMARK_SCENARIOS
from tests.e2e_agentic.scenarios.benchmark._pipeline import (
    EXPECTED_TRACE,
    FINAL_ARTIFACT_GLOB,
    INPUT_TYPES,
    INSTALL_ENV_CHANNELS,
    TOOLS,
    compose_benchmark_prompt,
    provision_for,
    shared_goal_block,
)
from tests.e2e_agentic.scenarios.benchmark.t1_install import InstallToolScenario


_A10 = DEFAULT_ARM             # metasmith / metasmith
_A7 = ARM_BY_ID["A7"]          # container / ad-hoc


def _prompt_ctx(sandbox: Path, arm) -> PromptContext:
    return PromptContext(
        sandbox=sandbox,
        version="0.18.4+deadbee",
        image_tag="quay.io/hallamlab/metasmith:0.18.4-deadbee",
        runtime="DOCKER",
        docs_dir=sandbox / "docs",
        tutorial_rel="",
        arm=arm,
    )


def _pipeline_scenarios():
    """The 6 pipeline scenarios (t2..t7); t1 is instantiated separately."""
    return [cls() for name, cls in BENCHMARK_SCENARIOS.items()
            if name != "t1_install"]


# ---------------------------------------------------------------------------
# import + dry-render for A10 and A7
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("arm", [_A10, _A7], ids=["A10", "A7"])
def test_pipeline_scenarios_render(tmp_path: Path, arm) -> None:
    sandbox = tmp_path / "sandbox"
    for scenario in _pipeline_scenarios():
        prompt = scenario.build_prompt(_prompt_ctx(sandbox, arm))
        assert isinstance(prompt, str) and prompt.strip()
        # The final-artifact deliverable is named in every pipeline prompt.
        assert "clusterProfiler" in prompt


@pytest.mark.parametrize("arm", [_A10, _A7], ids=["A10", "A7"])
def test_t1_install_renders_all_cells(tmp_path: Path, arm) -> None:
    sandbox = tmp_path / "sandbox"
    # env_channel matching the arm keeps the cell meaningful, but any renders.
    channel = "metasmith" if arm.is_metasmith else "container"
    for tool in TOOLS:
        scenario = InstallToolScenario(env_channel=channel, tool=tool)
        prompt = scenario.build_prompt(_prompt_ctx(sandbox, arm))
        assert isinstance(prompt, str) and prompt.strip()
        assert TOOLS[tool].binary in prompt


def test_t1_rejects_bad_params() -> None:
    with pytest.raises(ValueError):
        InstallToolScenario(env_channel="nope", tool="fastp")
    with pytest.raises(ValueError):
        InstallToolScenario(env_channel="container", tool="nope")


# ---------------------------------------------------------------------------
# prompt-symmetry invariant: shared block byte-identical across arms
# ---------------------------------------------------------------------------


def test_shared_block_is_byte_identical_across_arms(tmp_path: Path) -> None:
    sandbox = tmp_path / "sandbox"
    for scenario in _pipeline_scenarios():
        # The metasmith arm's composed prompt is the shared block verbatim.
        p10 = scenario.build_prompt(_prompt_ctx(sandbox, _A10))
        p7 = scenario.build_prompt(_prompt_ctx(sandbox, _A7))
        # A7 carries a non-empty preamble; the shared block is its exact suffix.
        assert p7.endswith(p10), (
            f"{scenario.name}: A7 prompt does not end with the A10 (shared) block"
        )
        assert p7 != p10, f"{scenario.name}: A7 preamble should differ from A10"


def test_metasmith_arm_prompt_has_no_preamble(tmp_path: Path) -> None:
    sandbox = tmp_path / "sandbox"
    for scenario in _pipeline_scenarios():
        shared = shared_goal_block(
            scenario.name, sandbox=sandbox, done_key=scenario.name,
            data_lines=scenario.data_lines(_prompt_ctx(sandbox, _A10)),
        )
        assert scenario.build_prompt(_prompt_ctx(sandbox, _A10)) == shared


# ---------------------------------------------------------------------------
# constants + dispatch sanity
# ---------------------------------------------------------------------------


def test_pipeline_constants_present() -> None:
    assert INPUT_TYPES == ("std::paired_reads_forward", "std::paired_reads_reverse")
    assert FINAL_ARTIFACT_GLOB.endswith("*.png")
    assert EXPECTED_TRACE[0] == INPUT_TYPES[0]
    assert set(INSTALL_ENV_CHANNELS) == {"ad-hoc", "mamba", "container", "metasmith"}


def test_provision_for_returns_callable_for_reference_arms() -> None:
    # A7 (container/ad-hoc) and A10 (metasmith) must yield a real callable.
    assert callable(provision_for(_A7))
    assert callable(provision_for(_A10))


def test_every_scenario_exposes_max_tokens_quota() -> None:
    # Per-test token quota field must exist on every scenario (pipeline + t1),
    # so run_cell's quota resolution can't silently regress. None until piloted.
    for sc in _pipeline_scenarios():
        assert hasattr(sc, "max_tokens"), sc.name
    assert hasattr(InstallToolScenario(env_channel="container", tool="fastp"),
                   "max_tokens")
