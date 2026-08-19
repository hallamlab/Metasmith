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


def test_scan_reads_both_arms_in_order():
    scan = ScanSource(_src("""
        context.ExecWithEnv() \\
            .ifContainerDo(env=image, cmd="a") \\
            .ifVirtualEnvDo(env=image, cmd="b")
    """))
    assert len(scan.chains) == 1
    assert scan.chains[0].arms == ["ifContainerDo", "ifVirtualEnvDo"]
    assert scan.chains[0].envs == ["image", "image"]
    assert scan.chains[0].cmds == ["'a'", "'b'"]
    assert scan.arms == ["ifContainerDo", "ifVirtualEnvDo"]


def test_scan_reads_a_container_only_transform():
    scan = ScanSource(_src('context.ExecWithEnv().ifContainerDo(env=image, cmd="a")'))
    assert scan.arms == ["ifContainerDo"]
    assert scan.empty_chains() == []


def test_scan_reads_positional_args():
    scan = ScanSource(_src('context.ExecWithEnv().ifVirtualEnvDo(image, "a")'))
    assert scan.chains[0].envs == ["image"]
    assert scan.chains[0].cmds == ["'a'"]


def test_scan_finds_multiple_independent_chains():
    scan = ScanSource(_src("""
        context.ExecWithEnv().ifContainerDo(env=a, cmd="one")
        context.ExecWithEnv().ifContainerDo(env=b, cmd="two").ifVirtualEnvDo(env=b, cmd="three")
    """))
    assert [c.arms for c in scan.chains] == [
        ["ifContainerDo"], ["ifContainerDo", "ifVirtualEnvDo"],
    ]


def test_scan_flags_an_armless_chain():
    scan = ScanSource(_src("context.ExecWithEnv()"))
    assert len(scan.empty_chains()) == 1


def test_scan_flags_the_retired_entry_point():
    scan = ScanSource(_src('context.ExecWithContainer(image=image, cmd="a")'))
    assert [n for n, _ in scan.forbidden] == ["ExecWithContainer"]


def test_scan_notes_host_shell_calls():
    scan = ScanSource(_src('context.external_shell.Exec("hostname")'))
    assert len(scan.host_shell_calls) == 1
    assert scan.chains == []


def test_duplicate_command_is_detected_only_across_two_arms():
    both = ScanSource(_src("""
        context.ExecWithEnv().ifContainerDo(env=i, cmd=cmd).ifVirtualEnvDo(env=i, cmd=cmd)
    """))
    assert both.chains[0].duplicate_command is True
    one = ScanSource(_src("context.ExecWithEnv().ifContainerDo(env=i, cmd=cmd)"))
    assert one.chains[0].duplicate_command is False


def test_validate_errors_on_the_retired_entry_point():
    res = check_env_declarations(_src('context.ExecWithContainer(image=i, cmd="a")'))
    assert res["errors"] and "ExecWithContainer" in res["errors"][0]


def test_validate_errors_on_an_armless_chain():
    res = check_env_declarations(_src("context.ExecWithEnv()"))
    assert res["errors"] and "declares no arm" in res["errors"][0]


def test_validate_errors_on_a_repeated_arm():
    res = check_env_declarations(_src("""
        context.ExecWithEnv().ifContainerDo(env=i, cmd="a").ifContainerDo(env=i, cmd="b")
    """))
    assert res["errors"] and "repeated arm" in res["errors"][0]


def test_validate_warns_but_does_not_error_on_identical_commands():
    res = check_env_declarations(_src("""
        context.ExecWithEnv().ifContainerDo(env=i, cmd=cmd).ifVirtualEnvDo(env=i, cmd=cmd)
    """))
    assert res["errors"] == []
    assert any("identical" in w for w in res["warnings"])


def test_validate_accepts_a_container_only_transform():
    res = check_env_declarations(_src('context.ExecWithEnv().ifContainerDo(env=i, cmd="a")'))
    assert res["errors"] == []
    assert res["arms"] == ["ifContainerDo"]


def test_validate_says_it_is_syntactic_only():
    assert check_env_declarations(_src("pass"))["syntactic_only"] is True


def _step(process: str, transform: str, arms, envs) -> dict:
    return {"step": 1, "transform": transform, "process": process, "arms": arms, "envs": envs}


def _mamba() -> Environment:
    return Environment(image="msm", runtime=Runtime.MAMBA)


def _docker() -> Environment:
    return Environment(image="docker://x", runtime=Runtime.DOCKER)


BOTH_ARMS = ["ifContainerDo", "ifVirtualEnvDo"]


def test_preflight_passes_a_fully_portable_plan():
    m = {"P1": _step("P1", "diamond", BOTH_ARMS, {"diamond.env": ["conda", "container"]})}
    _check_env_portability(m, _mamba())


def test_preflight_refuses_a_step_with_no_virtual_env_arm():
    m = {"P1": _step("P1", "interproscan", ["ifContainerDo"], {"ipr.env": ["container"]})}
    with pytest.raises(EnvPortabilityError) as e:
        _check_env_portability(m, _mamba())
    assert "interproscan" in str(e.value)
    assert "ifVirtualEnvDo" in str(e.value)


def test_preflight_refuses_an_env_resource_with_no_conda_entry():
    m = {"P1": _step("P1", "gtdbtk", BOTH_ARMS, {"gtdbtk.env": ["container"]})}
    with pytest.raises(EnvPortabilityError) as e:
        _check_env_portability(m, _mamba())
    assert "gtdbtk.env" in str(e.value)
    assert "conda" in str(e.value)


def test_preflight_lists_every_offending_step_not_just_the_first():
    m = {
        "P1": _step("P1", "alpha", ["ifContainerDo"], {}),
        "P2": _step("P2", "beta", BOTH_ARMS, {"b.env": ["container"]}),
        "P3": _step("P3", "gamma", BOTH_ARMS, {"c.env": ["conda"]}),
    }
    with pytest.raises(EnvPortabilityError) as e:
        _check_env_portability(m, _mamba())
    msg = str(e.value)
    assert "alpha" in msg and "beta" in msg and "gamma" not in msg


def test_preflight_is_a_no_op_under_a_container_runtime():
    m = {"P1": _step("P1", "interproscan", ["ifContainerDo"], {"ipr.env": ["container"]})}
    _check_env_portability(m, _docker())


def test_preflight_treats_an_unscannable_transform_as_unknown_not_absent():
    m = {"P1": _step("P1", "mystery", None, {})}
    _check_env_portability(m, _mamba())


def test_preflight_treats_an_unreadable_resource_as_unknown_not_absent():
    m = {"P1": _step("P1", "mystery", BOTH_ARMS, {"x.env": None})}
    _check_env_portability(m, _mamba())


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
    payload = json.dumps({"schema": 1, "steps": {"P1": _step("P1", "a", ["ifContainerDo"], {})}})
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
    old = {"P1": _step("P1", "gtdbtk", BOTH_ARMS, {"gtdbtk.env": ["container"]})}
    new = {"P1": _step("P1", "gtdbtk", BOTH_ARMS, {"gtdbtk.env": {"container": "docker://x"}})}
    for manifest in (old, new):
        with pytest.raises(EnvPortabilityError, match="conda"):
            _check_env_portability(manifest, _mamba())
        _check_env_portability(manifest, _docker())


def test_manifest_schema_advanced_with_the_recorded_shape():
    assert AgentPaths.ENV_MANIFEST_SCHEMA == 2
