"""Live: agent runs a single transform directly via `metasmith run`."""
from __future__ import annotations

import pytest

from tests.e2e_agentic.scenarios.story_run_direct import StoryRunDirectScenario


@pytest.mark.e2e_agentic
def test_story_run_direct(run_scenario):
    scenario = StoryRunDirectScenario()
    result, failures, log_dir = run_scenario(scenario, "DOCKER")
    assert not failures, (
        f"story_run_direct failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
