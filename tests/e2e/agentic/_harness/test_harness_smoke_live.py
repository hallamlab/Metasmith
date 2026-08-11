"""Live harness smoke — proves sandbox + driver + checkpoint wiring."""
from __future__ import annotations

import pytest

from tests.e2e.agentic.scenarios.harness_smoke import HarnessSmokeScenario


@pytest.mark.e2e_agentic
def test_harness_smoke_live(run_scenario):
    scenario = HarnessSmokeScenario()
    # DOCKER runtime arbitrarily — neither runtime actually fires here
    # because the agent only runs `msm --help`.
    result, failures, log_dir = run_scenario(scenario, "DOCKER")
    assert not failures, (
        f"harness_smoke failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
