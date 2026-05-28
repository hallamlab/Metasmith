"""Live e2e: agent drives the custom_transforms tutorial."""
from __future__ import annotations

import pytest

from tests.e2e_agentic.scenarios.custom_transforms import CustomTransformsScenario


@pytest.mark.e2e_agentic
@pytest.mark.parametrize("runtime", ["DOCKER", "APPTAINER"])
def test_custom_transforms(run_scenario, runtime):
    scenario = CustomTransformsScenario()
    result, failures, log_dir = run_scenario(scenario, runtime)
    assert not failures, (
        f"{scenario.name} ({runtime}) failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
