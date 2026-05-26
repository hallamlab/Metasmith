"""Live: agent authors a new data type from a clean workspace."""
from __future__ import annotations

import pytest

from tests.e2e_agentic.scenarios.story_author_type import StoryAuthorTypeScenario


@pytest.mark.e2e_agentic
def test_story_author_type(run_scenario):
    scenario = StoryAuthorTypeScenario()
    result, failures, log_dir = run_scenario(scenario, "DOCKER")
    assert not failures, (
        f"story_author_type failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
