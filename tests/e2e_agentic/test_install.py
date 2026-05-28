"""Live: agent installs metasmith from the spoofed `hallamlab` channel."""
from __future__ import annotations

import pytest

from tests.e2e_agentic.scenarios.install import InstallScenario


@pytest.mark.e2e_agentic
def test_install(run_scenario):
    scenario = InstallScenario()
    # Runtime axis is N/A for install (no container is exercised) — pick
    # DOCKER so the sandbox builder doesn't pre-place a sif we won't use.
    result, failures, log_dir = run_scenario(scenario, "DOCKER")
    assert not failures, (
        f"install failed; transcripts in {log_dir}\n"
        + "\n".join(f"  - {f}" for f in failures)
    )
