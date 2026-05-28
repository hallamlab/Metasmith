"""Live: agent recovers from a plan that emits a lineage_mismatch hint."""
from __future__ import annotations

import pytest

from tests.e2e_agentic.scenarios.recover_lineage_mismatch import (
    RecoverLineageMismatchScenario,
)


@pytest.mark.e2e_agentic
def test_recover_lineage_mismatch(run_scenario):
    scenario = RecoverLineageMismatchScenario()
    result, failures, log_dir = run_scenario(scenario, "DOCKER")
    assert not failures, (
        f"recover_lineage_mismatch failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
