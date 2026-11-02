import json
import textwrap
from pathlib import Path

import pytest

from metasmith.agents import (
    EnvPortabilityError,
    _check_env_portability,
    _read_env_manifest,
)
from metasmith.constants import AgentPaths
from metasmith.env import Environment, Runtime
from metasmith.env.dispatch_scan import ScanSource
from metasmith.ops.transforms import check_env_declarations


def _src(body: str) -> str:
    lines = textwrap.dedent(body).strip().split("\n")
    return "def protocol(context):\n" + "\n".join("    " + l for l in lines) + "\n"


def test_scan_reads_the_env_and_the_command():
    scan = ScanSource(_src('context.ExecWithEnv(env=image, cmd="a")'))
    assert len(scan.runs) == 1
    assert scan.runs[0].env == "image"
    assert scan.runs[0].cmd == "'a'"


def test_scan_reads_positional_args():
    scan = ScanSource(_src('context.ExecWithEnv(image, "a")'))
    assert scan.runs[0].env == "image"
    assert scan.runs[0].cmd == "'a'"


def test_scan_finds_every_independent_run_in_line_order():
    scan = ScanSource(_src("""
        context.ExecWithEnv(env=a, cmd="one")
        context.ExecWithEnv(env=b, cmd="two")
    """))
    assert [r.env for r in scan.runs] == ["a", "b"]


def test_scan_records_an_env_that_is_not_a_plain_name_as_unknown():
    # The env name is how the library recovers the module-level Dependency; an
    # expression cannot be resolved that way, and is recorded as unknown rather
    # than reported as absent.
    scan = ScanSource(_src('context.ExecWithEnv(env=envs[0], cmd="a")'))
    assert scan.runs[0].env is None
    assert scan.runs[0].has_env is True
    assert scan.incomplete() == []


def test_scan_flags_a_run_that_can_never_run_anything():
    scan = ScanSource(_src("context.ExecWithEnv(env=image)"))
    assert len(scan.incomplete()) == 1


def test_scan_flags_the_retired_entry_point():
    scan = ScanSource(_src('context.ExecWithContainer(image=image, cmd="a")'))
    assert [n for n, _ in scan.forbidden] == ["ExecWithContainer"]


def test_scan_flags_the_retired_arms_by_name():
    # An unmigrated body must fail by name. The alternative is that the chain is
    # simply not recognised, and its command silently never runs.
    scan = ScanSource(_src("""
        context.ExecWithEnv() \\
            .ifContainerDo(env=image, cmd="a") \\
            .ifVirtualEnvDo(env=image, cmd="b")
    """))
    assert sorted(n for n, _ in scan.forbidden) == ["ifContainerDo", "ifVirtualEnvDo"]


def test_scan_notes_host_shell_calls():
    scan = ScanSource(_src('context.external_shell.Exec("hostname")'))
    assert len(scan.host_shell_calls) == 1
    assert scan.runs == []


def test_validate_errors_on_the_retired_entry_point():
    res = check_env_declarations(_src('context.ExecWithContainer(image=i, cmd="a")'))
    assert res["errors"] and "ExecWithContainer" in res["errors"][0]


def test_validate_errors_on_a_retired_arm():
    res = check_env_declarations(
        _src('context.ExecWithEnv().ifContainerDo(env=i, cmd="a")')
    )
    assert any("ifContainerDo" in e for e in res["errors"])


def test_validate_errors_on_a_run_with_no_command():
    res = check_env_declarations(_src("context.ExecWithEnv(env=i)"))
    assert res["errors"] and "cmd" in res["errors"][0]


def test_validate_accepts_a_single_run():
    res = check_env_declarations(_src('context.ExecWithEnv(env=i, cmd="a")'))
    assert res["errors"] == []
    assert res["runs"] == [{"line": 2, "env": "i"}]


def test_validate_says_it_is_syntactic_only():
    assert check_env_declarations(_src("pass"))["syntactic_only"] is True


def _step(process: str, transform: str, runs, envs) -> dict:
    return {"step": 1, "transform": transform, "process": process, "runs": runs, "envs": envs}


def _mamba() -> Environment:
    return Environment(image="msm", runtime=Runtime.MAMBA)


def _docker() -> Environment:
    return Environment(image="docker://x", runtime=Runtime.DOCKER)


# ------------------------------------------------ the preflight asks about the env
#
# Not about the body. A body says what to run and in what environment; the runtime
# decides how. So the only way a step cannot run on this agent is that its
# environment resource declares nothing for this agent's runtime.


def test_preflight_passes_a_fully_portable_plan():
    m = {"P1": _step("P1", "diamond", 1, {"diamond.env": ["conda", "container"]})}
    _check_env_portability(m, _mamba())
    _check_env_portability(m, _docker())


def test_preflight_refuses_an_env_resource_with_no_conda_entry():
    m = {"P1": _step("P1", "gtdbtk", 1, {"gtdbtk.env": ["container"]})}
    with pytest.raises(EnvPortabilityError) as e:
        _check_env_portability(m, _mamba())
    assert "gtdbtk.env" in str(e.value)
    assert "conda" in str(e.value)


def test_preflight_refuses_an_env_resource_with_no_container_entry():
    # The direction that was never checked, and what let `cobra.env` ship with a
    # `conda:` line and nothing else -- unrunnable on the default agent runtime.
    m = {"P1": _step("P1", "cobra_fba", 1, {"cobra.env": ["conda"]})}
    with pytest.raises(EnvPortabilityError) as e:
        _check_env_portability(m, _docker())
    assert "cobra.env" in str(e.value)
    assert "container" in str(e.value)


def test_preflight_lists_every_offending_step_not_just_the_first():
    m = {
        "P1": _step("P1", "alpha", 1, {"a.env": ["container"]}),
        "P2": _step("P2", "beta", 1, {"b.env": ["container"]}),
        "P3": _step("P3", "gamma", 1, {"c.env": ["conda"]}),
    }
    with pytest.raises(EnvPortabilityError) as e:
        _check_env_portability(m, _mamba())
    msg = str(e.value)
    assert "alpha" in msg and "beta" in msg and "gamma" not in msg


def test_preflight_ignores_a_step_that_launches_no_tool():
    m = {"P1": _step("P1", "pure_python", 0, {"x.env": ["container"]})}
    _check_env_portability(m, _mamba())


def test_preflight_treats_an_unscannable_transform_as_unknown_not_absent():
    m = {"P1": _step("P1", "mystery", None, {})}
    _check_env_portability(m, _mamba())


def test_preflight_treats_a_manifest_from_an_older_metasmith_as_unknown():
    # No `runs` key at all: staged before the arms were collapsed. Indistinguishable
    # from "nothing to check", and treated as such -- otherwise every already-staged
    # workspace starts failing at run.
    m = {"P1": {"step": 1, "transform": "old", "process": "P1",
                "arms": ["ifContainerDo"], "envs": {"x.env": ["container"]}}}
    _check_env_portability(m, _mamba())


def test_preflight_treats_an_unreadable_resource_as_unknown_not_absent():
    m = {"P1": _step("P1", "mystery", 1, {"x.env": None})}
    _check_env_portability(m, _mamba())
    _check_env_portability(m, _docker())


def test_preflight_is_a_no_op_with_no_manifest():
    _check_env_portability({}, _mamba())


class _CatShell:
    def __init__(self, text: str | None):
        self.text = text

    def Exec(self, cmd, history=False, quiet=False, timeout=None):
        from metasmith.coms.terminals import ShellResult
        if self.text is None:
            return ShellResult(out=[], err=[])
        return ShellResult(out=self.text.split("\n"), err=[])


def test_manifest_read_returns_the_steps():
    payload = json.dumps({"schema": 1, "steps": {"P1": _step("P1", "a", 1, {})}})
    steps = _read_env_manifest(_CatShell(payload), Path("/ws"))
    assert list(steps) == ["P1"]


def test_missing_manifest_reads_as_nothing_to_check():
    assert _read_env_manifest(_CatShell(None), Path("/ws")) == {}


def test_unparseable_manifest_refuses_rather_than_degrading():
    with pytest.raises(EnvPortabilityError, match="could not be parsed"):
        _read_env_manifest(_CatShell('{"schema": 1, "steps": {oops}'), Path("/ws"))


def test_manifest_constant_is_distinct_from_the_gpu_one():
    assert AgentPaths.ENV_MANIFEST != AgentPaths.GPU_MANIFEST


# --------------------------------------------------- what a resource resolves to
#
# The manifest records which of `container:` / `conda:` each env resource
# carries, so the preflight can answer "can this agent run this step". A
# pre-flight *materialise* needs one thing more: the image itself, resolved at
# stage time, where the library is actually on disk. Recording it here is what
# lets the launch path answer "is this image in the store" without re-reading a
# transform library it does not have.


class _Dep:
    key = "env"


_DEP = _Dep()


class _Inst:
    def __init__(self, path, dtype_name="containers::tool.oci"):
        self._path = path
        self.dtype_name = dtype_name

    def ResolvePath(self):
        return self._path


class _Step:
    def __init__(self, *paths):
        from types import SimpleNamespace
        self.transform = SimpleNamespace(_env_deps=[_DEP])
        self.dependency_map = {_DEP: [_Inst(p) for p in paths]}


def _declarations(*paths):
    from metasmith.models.workflow.nextflow_codegen import _read_env_declarations
    return _read_env_declarations(_Step(*paths))


def test_declaration_records_both_resolved_values(tmp_path):
    p = tmp_path/"kraken2.env"
    p.write_text("container: docker://quay.io/biocontainers/kraken2:2.1.3--h43eeafb_0\nconda: kraken2-2.1.3\n")
    assert _declarations(p) == {"kraken2.env": {
        "container": "docker://quay.io/biocontainers/kraken2:2.1.3--h43eeafb_0",
        "conda": "kraken2-2.1.3",
    }}


def test_declaration_of_a_container_only_resource_records_no_conda(tmp_path):
    p = tmp_path/"ipr.env"
    p.write_text("container: docker://quay.io/biocontainers/interproscan:5.59\n")
    assert _declarations(p) == {"ipr.env": {
        "container": "docker://quay.io/biocontainers/interproscan:5.59",
    }}


def test_legacy_bare_uri_resolves_as_a_container_and_nothing_else(tmp_path):
    # A `*.oci` file whose whole content is a URI parses as a YAML scalar, not a
    # mapping. It is a container image and has no conda form -- which is exactly
    # what ResolveEnvImage does with it, and the reason this shares that helper
    # rather than parsing the file a second way.
    p = tmp_path/"tool.oci"
    p.write_text("docker://quay.io/example/tool:1.0\n")
    assert _declarations(p) == {"tool.oci": {"container": "docker://quay.io/example/tool:1.0"}}


def test_unreadable_resource_stays_unknown(tmp_path):
    assert _declarations(tmp_path/"missing.env") == {"containers::tool.oci": None}


def test_preflight_reads_both_manifest_generations(tmp_path):
    old = {"P1": _step("P1", "gtdbtk", 1, {"gtdbtk.env": ["container"]})}
    new = {"P1": _step("P1", "gtdbtk", 1, {"gtdbtk.env": {"container": "docker://x"}})}
    for manifest in (old, new):
        with pytest.raises(EnvPortabilityError, match="conda"):
            _check_env_portability(manifest, _mamba())
        _check_env_portability(manifest, _docker())


def test_manifest_schema_advanced_with_the_recorded_shape():
    assert AgentPaths.ENV_MANIFEST_SCHEMA == 2
