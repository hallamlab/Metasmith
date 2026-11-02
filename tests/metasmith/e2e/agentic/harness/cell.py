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
    sb_root.mkdir(parents=True, exist_ok=True)
    layout = build_sandbox(sb_root, ctx, runtime=runtime)

    agent_env = env_for_agent(layout)
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
