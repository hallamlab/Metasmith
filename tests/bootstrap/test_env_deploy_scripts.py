"""Characterize the Environment-owned deploy scripts (T2 seam).

`Agent.Deploy` no longer inlines the `msm` wrapper or the `msm_bootstrap`
launcher; it asks the `Environment` to render them. Container runtimes emit
the relay-bounce launcher (cross the container boundary); the relay-free
branch (mamba/native, T4) emits a direct in-process invocation with no
bounce, no `run_container`, and no `msm_relay`.

These pin the structural contract at the seam so the T4 mamba path can be
asserted to take *no* relay.
"""

from pathlib import Path

import pytest

from metasmith.env.environment import ContainerDef, Environment, Runtime


AH = Path("/arc/home/u/msm_home")
IMG = "docker://quay.io/hallamlab/metasmith:9.9.9-abc"
DEV_SRC = "$AGENT_HOME/dev/metasmith"
DEV_TARGET = "/opt/conda/envs/metasmith_env/lib/python3.12/site-packages/metasmith"

# The bootstrap stages the dev overlay to node-local scratch before binding it
# (SLURM array fan-out), so the mount it renders sources from $DEV_BIND_SRC --
# whatever the staging block left there -- and not from DEV_SRC.
STAGED_SRC = "$DEV_BIND_SRC"


def _dev_binds(env: Environment, src: str) -> str:
    # What deploy hands the wrapper, and what the bootstrap derives internally.
    return Environment(
        image=env.image,
        runtime=env.runtime,
        native=env.native,
        container=ContainerDef(binds=[(src, Path(DEV_TARGET))]),
    ).MakeBindsParam()


def _container(runtime: Runtime, **kw) -> Environment:
    return Environment(
        image=IMG,
        runtime=runtime,
        container=ContainerDef(
            cache=Path("$AGENT_HOME") / "container_images",
            workdir=kw.pop("workdir", None),
        ),
        **kw,
    )


def _render_wrapper(env: Environment) -> str:
    return env.RenderMsmWrapper(
        agent_home=AH,
        run_command=env.MakeRunCommand(local=True, custom_bind_param="$BINDS"),
        main_binds=env.MakeBindsParam(),
        dev_binds=_dev_binds(env, DEV_SRC),
        dev_src=DEV_SRC,
    )


def _render_bootstrap(env: Environment) -> str:
    return env.RenderBootstrap(
        agent_home=AH,
        run_command=env.MakeRunCommand(local=True, custom_bind_param="$BINDS"),
        run_binds=env.MakeBindsParam(),
        dev_src=DEV_SRC,
        dev_target=DEV_TARGET,
        bind_file="binds.txt",
    )


@pytest.mark.parametrize("runtime", [Runtime.DOCKER, Runtime.APPTAINER])
class TestContainerScripts:
    def test_container_needs_relay(self, runtime):
        assert _container(runtime).needs_relay is True

    def test_bootstrap_has_relay_bounce_and_daemon(self, runtime):
        script = _render_bootstrap(_container(runtime, workdir=Path("/ws")))
        # The bounce header crosses the container boundary back to the host.
        assert "bouncing to external" in script
        assert "msm_relay.x86_64-linux --io" in script
        # The per-step container launcher + the relay daemon lifecycle.
        assert "function run_container" in script
        assert "msm_relay start --local" in script
        assert "msm_relay stop" in script
        # And it still ultimately executes the transform step.
        assert "metasmith api execute_transform" in script

    def test_wrapper_runs_metasmith_in_container(self, runtime):
        script = _render_wrapper(_container(runtime))
        assert "metasmith $@" in script
        # The run-command is the container exec, carrying $BINDS.
        assert "$BINDS" in script
        assert runtime.value in script  # "docker" / "apptainer"

    def test_container_scripts_do_not_override_the_roots(self, runtime):
        # Under a container the agent home is bound at /msm_home and the cwd at
        # /ws, so the defaults are already correct. Exporting an override here
        # would be read *inside* the container and point at a host path.
        for script in (
            _render_wrapper(_container(runtime)),
            _render_bootstrap(_container(runtime, workdir=Path("/ws"))),
        ):
            assert "METASMITH_HOME_ROOT" not in script
            assert "METASMITH_WORK_ROOT" not in script

    def test_bootstrap_bounce_uses_container_literals(self, runtime):
        # The bounce test and the relay io path run inside the container, so
        # they must interpolate the fixed layout, never the resolvable roots.
        script = _render_bootstrap(_container(runtime, workdir=Path("/ws")))
        assert 'if [ -e "/msm_home" ]; then' in script
        assert "--io /msm_home/relay/" in script


@pytest.mark.parametrize("runtime", [Runtime.DOCKER, Runtime.APPTAINER])
class TestBindDialectPurity:
    """Every mount in a rendered script must be spelled by `MakeBindsParam` for
    that runtime. Hand-writing a flag is how the dev-overlay mount ended up
    emitting apptainer's `--bind` into a docker command line."""

    # Tokens that only ever belong to the *other* runtime's dialect. The
    # trailing space on `--bind ` keeps `--bind_file`-alikes from matching.
    FOREIGN = {
        Runtime.DOCKER: ["--bind "],
        Runtime.APPTAINER: ["--mount ", "type=bind,"],
    }

    def test_scripts_carry_no_foreign_dialect(self, runtime):
        scripts = {
            "wrapper": _render_wrapper(_container(runtime)),
            "bootstrap": _render_bootstrap(_container(runtime, workdir=Path("/ws"))),
        }
        for name, script in scripts.items():
            for token in self.FOREIGN[runtime]:
                assert token not in script, f"{runtime.value} {name} carries [{token}]"

    def test_dev_overlay_mount_is_dialect_correct(self, runtime):
        # Positive control: without this the scan above passes vacuously on a
        # render that emits no mounts at all.
        bootstrap = _render_bootstrap(_container(runtime, workdir=Path("/ws")))
        # The bootstrap binds the *staged* copy, not the shared source tree --
        # binding the latter silently undoes the errno-108 fan-out fix and is
        # invisible outside a large SLURM array.
        assert f'BINDS="$BINDS {_dev_binds(_container(runtime), STAGED_SRC)}"' in bootstrap
        assert _dev_binds(_container(runtime), DEV_SRC) in _render_wrapper(_container(runtime))


def test_apptainer_dev_overlay_line_is_pinned():
    # Exact bytes of the line the e2e-validated HPC path runs. Pinned rather
    # than derived so a change to `MakeBindsParam` cannot move it silently.
    bootstrap = _render_bootstrap(_container(Runtime.APPTAINER, workdir=Path("/ws")))
    assert f'BINDS="$BINDS --bind $DEV_BIND_SRC:{DEV_TARGET}"' in bootstrap


class TestRelayFreeScripts:
    """mamba/native cross no boundary, so nothing is mounted at the container
    roots; the deployed scripts must hand metasmith the real host paths."""

    def _mamba(self) -> Environment:
        return Environment(image="msm_tool_env", runtime=Runtime.MAMBA)

    def test_mamba_needs_no_relay(self):
        assert self._mamba().needs_relay is False

    def test_wrapper_exports_agent_home_and_both_roots(self):
        script = _render_wrapper(self._mamba())
        assert f"export AGENT_HOME={AH}" in script
        # The msm wrapper carries no workdir; deploy dual-binds the agent home
        # at both roots in the container case, so both point at it here too.
        assert f"export METASMITH_HOME_ROOT={AH}" in script
        assert f"export METASMITH_WORK_ROOT={AH}" in script
        assert "mamba run -n msm_tool_env metasmith $@" in script

    def test_bootstrap_exports_home_root_and_cwd_work_root(self):
        script = _render_bootstrap(self._mamba())
        assert f"export AGENT_HOME={AH}" in script
        assert f"export METASMITH_HOME_ROOT={AH}" in script
        # /ws is bound to the step's cwd in the container case; without a
        # container the work root IS that cwd.
        assert 'export METASMITH_WORK_ROOT="$(pwd -P)"' in script
        # ... and it must be exported after the `cd`, or it captures the
        # launch directory instead of the step's.
        assert script.index("cd $CWD") < script.index("export METASMITH_WORK_ROOT")

    def test_bootstrap_takes_no_relay(self):
        script = _render_bootstrap(self._mamba())
        assert "msm_relay" not in script
        assert "bouncing to external" not in script
        assert "run_container" not in script
        assert "metasmith api execute_transform" in script
