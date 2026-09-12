from __future__ import annotations

import pytest

from tests.metasmith.e2e.agentic.scenarios.recover_lineage_mismatch import (
    RecoverLineageMismatchScenario,
)
from tests.metasmith.e2e.agentic.scenarios.recover_unreachable_target import (
    RecoverUnreachableTargetScenario,
)


_RECOVERY_CASES: list[tuple[str, callable]] = [
    ("recover_lineage_mismatch",   RecoverLineageMismatchScenario),
    ("recover_unreachable_target", RecoverUnreachableTargetScenario),
]


@pytest.mark.parametrize(
    "scenario_factory",
    [factory for _, factory in _RECOVERY_CASES],
    ids=[case_id for case_id, _ in _RECOVERY_CASES],
)
def test_agent_recovery(run_scenario, scenario_factory):
    scenario = scenario_factory()
    result, failures, log_dir = run_scenario(scenario, "DOCKER")
    assert not failures, (
        f"{scenario.name} failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
