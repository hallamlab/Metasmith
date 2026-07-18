"""Pytest wiring for the e2e_agentic suite."""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest

from tests.e2e_agentic.drivers.factory import make_driver
from tests.e2e_agentic.install_mock.verify_local_artifacts import (
    PreflightError,
    verify as verify_install,
)


_METASMITH_ENV_NAME = "msm_env"


def pytest_addoption(parser):
    g = parser.getgroup("e2e_agentic")
    g.addoption("--agent", default="opencode",
                choices=("opencode", "claude"),
                help="agent driver (default: opencode)")
    g.addoption("--agent-model", default=None,
                help="model id (default: driver-specific)")
    g.addoption("--agent-effort", default=None,
                help="effort level (claude only)")
    g.addoption("--max-iters", type=int, default=20)
    # None → the scenario's own max_tokens quota, else the global fallback.
    g.addoption("--max-tokens", type=int, default=None)
    g.addoption("--max-tokens-per-iter", type=int, default=200_000)
    g.addoption("--max-usd-per-iter", type=float, default=None,
                help="per-invocation dollar runaway valve (claude "
                     "--max-budget-usd); None → derived from --max-tokens-per-iter")
    g.addoption("--iter-timeout-s", type=float, default=300.0,
                help="per-iteration wall-clock timeout (claude + opencode); "
                     "keep tight so a hung workflow (e.g. nextflow stuck at "
                     "JVM init) surfaces in minutes instead of waiting out "
                     "the agent's hard-coded 1h `workflow wait --timeout`")
    g.addoption("--dry-run", action="store_true",
                help="render the prompt + argv without spawning the agent")
    g.addoption("--runs-dir", default=None,
                help="root for transcripts (default: tests/e2e_agentic/.runs/<ts>)")
    from tests.e2e_agentic.scenarios.arms import ARMS, DEFAULT_ARM
    g.addoption("--arm", default=DEFAULT_ARM.id,
                choices=tuple(a.id for a in ARMS),
                help="env × orchestrator arm to run (default: %(default)s = full "
                     "metasmith). See scenarios/arms.py.")


def pytest_configure(config):
    config.addinivalue_line("markers",
        "e2e_agentic: live agent-driven run (opt-in; needs API key)")


def pytest_collection_modifyitems(config, items):
    """Skip e2e_agentic tests up-front if MetasmithLibraries is unobtainable.

    The fixture path resolves a MetasmithLibraries checkout via env var,
    sibling dir, or auto-clone. If all three fail (e.g. CI without git or
    network), there's no way the scenarios can run — surface a clear skip
    reason at collection time rather than letting setup_fixtures explode.
    """
    from tests.e2e_agentic.scenarios._fixture_utils import _metasmith_libraries_root
    try:
        _metasmith_libraries_root()
    except RuntimeError as exc:
        skip = pytest.mark.skip(reason=f"MetasmithLibraries unavailable: {exc}")
        for item in items:
            if "e2e_agentic" in item.keywords:
                item.add_marker(skip)


# ---------------------------------------------------------------------------
# session-scoped: install context + runs dir
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def runs_dir(project_root, pytestconfig) -> Path:
    explicit = pytestconfig.getoption("--runs-dir")
    if explicit:
        return Path(explicit).resolve()
    ts = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    p = project_root / "tests" / "e2e_agentic" / ".runs" / ts
    p.mkdir(parents=True, exist_ok=True)
    return p


@pytest.fixture(scope="session")
def install_context_factory(project_root, pytestconfig):
    """Returns a callable that produces an InstallContext for a runtime."""
    def _make(runtime: str):
        agent = pytestconfig.getoption("--agent")
        try:
            return verify_install(
                project_root, runtime=runtime, agent=agent,
            )
        except PreflightError as exc:
            pytest.skip(f"preflight failed: {exc}")
    return _make


# ---------------------------------------------------------------------------
# per-test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def agent_driver(pytestconfig):
    name = pytestconfig.getoption("--agent")
    model = pytestconfig.getoption("--agent-model")
    opts: dict = {}
    effort = pytestconfig.getoption("--agent-effort")
    if effort and name == "claude":
        opts["effort"] = effort
    opts["timeout_s"] = pytestconfig.getoption("--iter-timeout-s")
    return make_driver(name, model=model, **opts)


@pytest.fixture
def loop_budgets(pytestconfig):
    from tests.e2e_agentic.harness.loop import LoopBudgets
    from tests.e2e_agentic.run_cell import _GLOBAL_MAX_TOKENS_FALLBACK
    # max_tokens stays a placeholder here (the scenario isn't known yet); the
    # per-scenario quota is resolved in run_scenario._run before ralph_loop.
    cli_max = pytestconfig.getoption("--max-tokens")
    return LoopBudgets(
        max_iters=pytestconfig.getoption("--max-iters"),
        max_tokens=cli_max if cli_max is not None else _GLOBAL_MAX_TOKENS_FALLBACK,
        max_tokens_per_iter=pytestconfig.getoption("--max-tokens-per-iter"),
        max_usd_per_iter=pytestconfig.getoption("--max-usd-per-iter"),
    )


# ---------------------------------------------------------------------------
# helper: run a scenario end-to-end
# ---------------------------------------------------------------------------


@pytest.fixture
def run_scenario(install_context_factory, agent_driver, tmp_path,
                 loop_budgets, runs_dir, pytestconfig):
    """High-level scenario runner.

    1. Resolve InstallContext for the runtime.
    2. Build the spoofed sandbox (copies docs/data_types/transforms,
       materializes .condarc, hardlinks the conda channel, pre-places the
       metasmith sif for APPTAINER, ensures bootstrap env).
    3. Optionally pre-install metasmith into the sandbox env
       (scenarios with ``pre_install_metasmith = True``).
    4. Render prompt, dispatch ralph_loop, run scenario.verify().
    """
    from tests.e2e_agentic.harness.cell import prepare_sandbox
    from tests.e2e_agentic.harness.loop import ralph_loop
    from tests.e2e_agentic.scenarios.arms import ARM_BY_ID
    from tests.e2e_agentic.scenarios.base import PromptContext, VerifyContext

    arm = ARM_BY_ID[pytestconfig.getoption("--arm")]

    def _run(scenario, runtime: str):
        ctx = install_context_factory(runtime)
        sb_root = tmp_path / "sandbox"
        # Build the sandbox + provision the arm via the shared helper (the same
        # path run_cell.py uses), so the fixture and the CLI never drift.
        layout, agent_env = prepare_sandbox(
            sb_root, ctx,
            runtime=runtime, scenario=scenario, arm=arm,
            metasmith_env_name=_METASMITH_ENV_NAME,
        )

        prompt_ctx = PromptContext(
            sandbox=sb_root,
            version=ctx.version,
            image_tag=ctx.image_tag,
            runtime=runtime,
            docs_dir=layout.docs,
            tutorial_rel=scenario.tutorial_path,
            arm=arm,
        )
        prompt = scenario.build_prompt(prompt_ctx)

        log_dir = runs_dir / scenario.name / runtime
        log_dir.mkdir(parents=True, exist_ok=True)

        if pytestconfig.getoption("--dry-run"):
            (log_dir / "PROMPT.md").write_text(prompt)
            pytest.skip(f"--dry-run: prompt rendered to {log_dir / 'PROMPT.md'}")

        # Re-resolve the token quota now that the scenario is known: explicit
        # --max-tokens wins, else the scenario's own quota, else the fallback
        # already baked into loop_budgets.
        from dataclasses import replace
        from tests.e2e_agentic.run_cell import (
            _GLOBAL_MAX_TOKENS_FALLBACK, _effective_max_tokens,
        )
        budgets = replace(loop_budgets, max_tokens=_effective_max_tokens(
            pytestconfig.getoption("--max-tokens"), scenario,
            _GLOBAL_MAX_TOKENS_FALLBACK,
        ))

        result = ralph_loop(
            driver=agent_driver,
            prompt=prompt,
            sandbox=sb_root,
            budgets=budgets,
            log_dir=log_dir,
            env=agent_env,
        )

        vctx = VerifyContext(
            sandbox=sb_root,
            agent_env=agent_env,
            metasmith_env_name=_METASMITH_ENV_NAME,
            installed_env_path=sb_root / "envs" / _METASMITH_ENV_NAME,
            arm=arm,
        )
        failures = scenario.verify(vctx, result)
        return result, failures, log_dir

    return _run
