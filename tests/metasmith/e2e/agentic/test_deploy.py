"""Live: agent runs `metasmith agent save` + `agent deploy` against the
locally-built container."""
from __future__ import annotations

import pytest

from tests.metasmith.e2e.agentic.scenarios.deploy import DeployScenario


@pytest.mark.e2e_agentic
@pytest.mark.parametrize("runtime", ["DOCKER", "APPTAINER"])
def test_deploy(run_scenario, runtime):
    scenario = DeployScenario()
    result, failures, log_dir = run_scenario(scenario, runtime)
    assert not failures, (
        f"deploy ({runtime}) failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
