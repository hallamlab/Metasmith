"""Live: agent browses pre-staged type/transform libraries."""
from __future__ import annotations

import pytest

from tests.e2e_agentic.scenarios.story_browse_libraries import (
    StoryBrowseLibrariesScenario,
)


@pytest.mark.e2e_agentic
def test_story_browse_libraries(run_scenario):
    scenario = StoryBrowseLibrariesScenario()
    result, failures, log_dir = run_scenario(scenario, "DOCKER")
    assert not failures, (
        f"story_browse_libraries failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
