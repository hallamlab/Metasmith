"""Live: agent recovers from a plan that dead-ends on a missing input."""
from __future__ import annotations

import pytest

from tests.e2e_agentic.scenarios.recover_unreachable_target import (
    RecoverUnreachableTargetScenario,
)


@pytest.mark.e2e_agentic
def test_recover_unreachable_target(run_scenario):
    scenario = RecoverUnreachableTargetScenario()
    result, failures, log_dir = run_scenario(scenario, "DOCKER")
    assert not failures, (
        f"recover_unreachable_target failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
