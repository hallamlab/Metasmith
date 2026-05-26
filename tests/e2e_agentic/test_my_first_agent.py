"""Live e2e: agent drives the my_first_agent tutorial."""
from __future__ import annotations

import pytest

from tests.e2e_agentic.scenarios.my_first_agent import MyFirstAgentScenario


@pytest.mark.e2e_agentic
@pytest.mark.parametrize("runtime", ["DOCKER", "APPTAINER"])
def test_my_first_agent(run_scenario, runtime):
    scenario = MyFirstAgentScenario()
    result, failures, log_dir = run_scenario(scenario, runtime)
    assert not failures, (
        f"{scenario.name} ({runtime}) failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
