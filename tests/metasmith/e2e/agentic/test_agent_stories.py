from __future__ import annotations

import pytest

from tests.metasmith.e2e.agentic.scenarios.custom_transforms import CustomTransformsScenario
from tests.metasmith.e2e.agentic.scenarios.my_first_agent import MyFirstAgentScenario
from tests.metasmith.e2e.agentic.scenarios.story_author_type import StoryAuthorTypeScenario
from tests.metasmith.e2e.agentic.scenarios.story_browse_libraries import (
    StoryBrowseLibrariesScenario,
)
from tests.metasmith.e2e.agentic.scenarios.story_plan_and_inspect import (
    StoryPlanAndInspectScenario,
)
from tests.metasmith.e2e.agentic.scenarios.story_run_direct import StoryRunDirectScenario


_SCENARIO_CASES: list[tuple[str, callable, str]] = [
    # User-story probes — single runtime; the assertions don't depend on
    # the container backend, just on the CLI surface.
    ("story_author_type",        StoryAuthorTypeScenario,        "DOCKER"),
    ("story_browse_libraries",   StoryBrowseLibrariesScenario,   "DOCKER"),
    ("story_plan_and_inspect",   StoryPlanAndInspectScenario,    "DOCKER"),
    ("story_run_direct",         StoryRunDirectScenario,         "DOCKER"),
    # Tutorial walk-throughs — sweep both runtimes; sandbox vs sif decision
    # matters here because the workflow actually runs.
    ("my_first_agent[DOCKER]",   MyFirstAgentScenario,           "DOCKER"),
    ("my_first_agent[APPTAINER]", MyFirstAgentScenario,          "APPTAINER"),
    ("custom_transforms[DOCKER]", CustomTransformsScenario,      "DOCKER"),
    ("custom_transforms[APPTAINER]", CustomTransformsScenario,   "APPTAINER"),
]


@pytest.mark.parametrize(
    ("scenario_factory", "runtime"),
    [(factory, runtime) for _, factory, runtime in _SCENARIO_CASES],
    ids=[case_id for case_id, _, _ in _SCENARIO_CASES],
)
def test_agent_story(run_scenario, scenario_factory, runtime):
    scenario = scenario_factory()
    result, failures, log_dir = run_scenario(scenario, runtime)
    assert not failures, (
        f"{scenario.name} ({runtime}) failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
