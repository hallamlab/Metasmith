"""Live: agent plans + inspects a workflow against pre-staged libs."""
from __future__ import annotations

import pytest

from tests.e2e_agentic.scenarios.story_plan_and_inspect import (
    StoryPlanAndInspectScenario,
)


@pytest.mark.e2e_agentic
def test_story_plan_and_inspect(run_scenario):
    scenario = StoryPlanAndInspectScenario()
    result, failures, log_dir = run_scenario(scenario, "DOCKER")
    assert not failures, (
        f"story_plan_and_inspect failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
