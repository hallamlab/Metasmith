"""Shared build+provision plumbing for one benchmark cell.

This is the seam both the pytest ``run_scenario`` fixture (``conftest.py``) and
the standalone ``run_cell`` CLI (``tests/e2e/agentic/run_cell.py``) call, so the
two never drift: one resolve → build_sandbox → (pre-install) → provision path,
authored once.

Nothing here spawns an agent or verifies an oracle — those stay with the caller
(the fixture / the CLI), which own the loop budgets, log dirs, and result
recording. This module only materializes the sandbox and provisions the arm.
"""
from __future__ import annotations

import inspect as _inspect
from pathlib import Path

from tests.metasmith.e2e.agentic.harness.sandbox import (
    SandboxLayout,
    build_sandbox,
    env_for_agent,
    install_metasmith_into_sandbox,
)
from tests.metasmith.e2e.agentic.install_mock.verify_local_artifacts import InstallContext
from tests.metasmith.e2e.agentic.scenarios.arms import Arm

#: The conda env name metasmith is (pre)installed into inside the sandbox.
DEFAULT_METASMITH_ENV_NAME = "msm_env"


def prepare_sandbox(
    sb_root: Path,
    ctx: InstallContext,
    *,
    runtime: str,
    scenario,
    arm: Arm,
    metasmith_env_name: str = DEFAULT_METASMITH_ENV_NAME,
    do_pre_install: bool = True,
) -> tuple[SandboxLayout, dict[str, str]]:
    """Build the spoofed sandbox and provision ``arm``'s start-state into it.

    Mirrors the step sequence the ``run_scenario`` fixture performs:

      1. ``build_sandbox`` — docs/data_types/transforms copies, spoofed
         ``.condarc``, hardlinked conda channel, pre-placed sif (APPTAINER).
      2. ``env_for_agent`` — the env dict the agent's shells inherit, plus the
         ``MSM_E2E_RUNTIME`` / ``MSM_E2E_IMAGE_TAG`` threadthroughs.
      3. Optional metasmith pre-install (scenarios with
         ``pre_install_metasmith``); prepends the installed env's ``bin/`` to
         PATH so ``metasmith`` / ``msm`` resolve regardless of shell activation.
      4. ``scenario.setup_fixtures`` — threading ``arm`` when the signature
         accepts it (benchmark scenarios) else the legacy ``(layout, ctx)``.
      5. Generic ``arm.provision`` hook (no-op for the canonical ARMS).

    ``do_pre_install=False`` skips the (slow, network-bound) ``mamba create``
    step — used by the CLI's ``--dry-run`` path, which only needs the prompt +
    provisioned start-state, not an installed metasmith.

    Returns ``(layout, agent_env)``.
    """
    sb_root.mkdir(parents=True, exist_ok=True)
    layout = build_sandbox(sb_root, ctx, runtime=runtime)

    agent_env = env_for_agent(layout)
    # Threadthrough for verifiers that recheck spoof targets (e.g. test_deploy).
    agent_env["MSM_E2E_RUNTIME"] = runtime
    agent_env["MSM_E2E_IMAGE_TAG"] = ctx.image_tag

    if do_pre_install and getattr(scenario, "pre_install_metasmith", False):
        install_metasmith_into_sandbox(layout, ctx, env_name=metasmith_env_name)
        env_bin = sb_root / "envs" / metasmith_env_name / "bin"
        agent_env["PATH"] = f"{env_bin}:{agent_env.get('PATH', '')}"

    if hasattr(scenario, "setup_fixtures"):
        sig = _inspect.signature(scenario.setup_fixtures)
        if "arm" in sig.parameters:
            scenario.setup_fixtures(layout, ctx, arm=arm)
        else:
            scenario.setup_fixtures(layout, ctx)

    if getattr(arm, "provision", None) is not None:
        arm.provision(layout, ctx)

    return layout, agent_env
