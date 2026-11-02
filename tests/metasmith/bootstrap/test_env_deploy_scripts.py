from pathlib import Path

import pytest

from metasmith.env.environment import ContainerDef, Environment, Runtime


AH = Path("/arc/home/u/msm_home")
IMG = "docker://quay.io/hallamlab/metasmith:9.9.9-abc"
DEV_SRC = "$AGENT_HOME/dev/metasmith"
DEV_TARGET = "/opt/conda/envs/metasmith_env/lib/python3.12/site-packages/metasmith"

STAGED_SRC = "$DEV_BIND_SRC"


def _dev_binds(env: Environment, src: str) -> str:
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
        assert "bouncing to external" in script
        assert "msm_relay.x86_64-linux --io" in script
        assert "function run_container" in script
        assert "msm_relay start --local" in script
        assert "msm_relay stop" in script
        assert "metasmith api execute_transform" in script

    def test_wrapper_runs_metasmith_in_container(self, runtime):
        script = _render_wrapper(_container(runtime))
        assert "metasmith $@" in script
        assert "$BINDS" in script
        assert runtime.value in script

    def test_container_scripts_do_not_override_the_roots(self, runtime):
        for script in (
            _render_wrapper(_container(runtime)),
            _render_bootstrap(_container(runtime, workdir=Path("/ws"))),
        ):
            assert "METASMITH_HOME_ROOT" not in script
            assert "METASMITH_WORK_ROOT" not in script

    def test_bootstrap_bounce_uses_container_literals(self, runtime):
        script = _render_bootstrap(_container(runtime, workdir=Path("/ws")))
        assert 'if [ -e "/msm_home" ]; then' in script
        assert "--io /msm_home/relay/" in script


@pytest.mark.parametrize("runtime", [Runtime.DOCKER, Runtime.APPTAINER])
class TestBindDialectPurity:
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
        bootstrap = _render_bootstrap(_container(runtime, workdir=Path("/ws")))
        assert f'BINDS="$BINDS {_dev_binds(_container(runtime), STAGED_SRC)}"' in bootstrap
        assert _dev_binds(_container(runtime), DEV_SRC) in _render_wrapper(_container(runtime))


def test_apptainer_dev_overlay_line_is_pinned():
    bootstrap = _render_bootstrap(_container(Runtime.APPTAINER, workdir=Path("/ws")))
    assert f'BINDS="$BINDS --bind $DEV_BIND_SRC:{DEV_TARGET}"' in bootstrap


class TestRelayFreeScripts:
    def _mamba(self) -> Environment:
        return Environment(image="msm_tool_env", runtime=Runtime.MAMBA)

    def test_mamba_needs_no_relay(self):
        assert self._mamba().needs_relay is False

    def test_wrapper_exports_agent_home_and_both_roots(self):
        script = _render_wrapper(self._mamba())
        assert f"export AGENT_HOME={AH}" in script
        assert f"export METASMITH_HOME_ROOT={AH}" in script
        assert f"export METASMITH_WORK_ROOT={AH}" in script
        assert "mamba run -n msm_tool_env metasmith $@" in script

    def test_bootstrap_exports_home_root_and_cwd_work_root(self):
        script = _render_bootstrap(self._mamba())
        assert f"export AGENT_HOME={AH}" in script
        assert f"export METASMITH_HOME_ROOT={AH}" in script
        assert 'export METASMITH_WORK_ROOT="$(pwd -P)"' in script
        assert script.index("cd $CWD") < script.index("export METASMITH_WORK_ROOT")

    def test_bootstrap_takes_no_relay(self):
        script = _render_bootstrap(self._mamba())
        assert "msm_relay" not in script
        assert "bouncing to external" not in script
        assert "run_container" not in script
        assert "metasmith api execute_transform" in script
