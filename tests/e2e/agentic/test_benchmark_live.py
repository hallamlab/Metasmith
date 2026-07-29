"""Live (opt-in) benchmark runs, driven through the real harness.

Marked ``e2e_agentic`` so they are deselected from the default unit suite. Use
these to drive a real (or ``--dry-run``) benchmark cell:

    pytest tests/e2e/agentic/test_benchmark_live.py --dry-run --arm A7 -q
    pytest tests/e2e/agentic/test_benchmark_live.py::test_t3_run --arm A10

The arm is chosen via the ``--arm`` option (default A10); ``run_scenario`` builds
the sandbox, provisions the arm's start-state, renders the prompt, runs the ralph
loop, and verifies. ``--dry-run`` stops after rendering the prompt.
"""
from __future__ import annotations

import pytest

from tests.e2e.agentic.scenarios.benchmark import (
    AddToolScenario,
    AdaptHpcScenario,
    AdaptNewHostScenario,
    FromMiddleScenario,
    InstallToolScenario,
    PipelineScenario,
    RunScenario,
)


def _assert_ok(scenario, run_scenario, runtime="DOCKER"):
    result, failures, log_dir = run_scenario(scenario, runtime)
    assert not failures, (
        f"{scenario.name} failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )


@pytest.mark.e2e_agentic
def test_t3_run(run_scenario):
    # The reference cell — fully real for A10 (metasmith) and A7 (container/ad-hoc).
    _assert_ok(RunScenario(), run_scenario)


@pytest.mark.e2e_agentic
def test_t2_pipeline(run_scenario):
    _assert_ok(PipelineScenario(), run_scenario)


@pytest.mark.e2e_agentic
def test_t4_adapt_new_host(run_scenario):
    _assert_ok(AdaptNewHostScenario(), run_scenario)


@pytest.mark.e2e_agentic
def test_t5_adapt_hpc(run_scenario):
    _assert_ok(AdaptHpcScenario(), run_scenario)


@pytest.mark.e2e_agentic
def test_t6_adapt_add_tool(run_scenario):
    _assert_ok(AddToolScenario(), run_scenario)


@pytest.mark.e2e_agentic
def test_t7_adapt_from_middle(run_scenario):
    _assert_ok(FromMiddleScenario(), run_scenario)


@pytest.mark.e2e_agentic
@pytest.mark.parametrize("tool", ["fastp", "spades", "bakta",
                                  "eggnog-mapper", "clusterprofiler", "abricate"])
@pytest.mark.parametrize("env_channel", ["ad-hoc", "mamba", "container", "metasmith"])
def test_t1_install(run_scenario, env_channel, tool):
    _assert_ok(InstallToolScenario(env_channel=env_channel, tool=tool), run_scenario)
