from __future__ import annotations

from pathlib import Path

from metasmith.bootstrap import _as_home_rooted
from metasmith.constants import AgentPaths
from metasmith.models.paths import PathMap
from metasmith.testing.contract_runtime import (
    CompiledTask,
    check_emitted_addresses,
)


HOME = Path("/msm_home")
WORK = Path("/ws")


def _compiled(workspace: Path, nf: str) -> CompiledTask:
    return CompiledTask(
        workspace=workspace,
        key="K",
        task=None,  # type: ignore[arg-type]  — unused by this checker
        workflow_nf=nf,
        home_root=HOME,
        work_root=WORK,
        external_home=Path("/scratch/agent"),
    )


def test_home_and_work_rooted_literals_pass(tmp_path):
    nf = (
        "workflow {\n"
        "def __cached_step_1 = [Channel.of([[:], "
        "file('/msm_home/task_cache/1e/20ab/out/f.gbk')])]\n"
        "def __cached_step_2 = [Channel.of([[:], file('/ws/work/aa/bb/x')])]\n"
        "}\n"
    )
    assert check_emitted_addresses(_compiled(tmp_path, nf)) == []


def test_host_rooted_cache_literal_is_reported(tmp_path):
    nf = (
        "workflow {\n"
        "def __cached_step_1 = [Channel.of([[:], "
        "file('/scratch/agent/task_cache/1e/20ab/out/f.gbk')])]\n"
        "}\n"
    )
    violations = check_emitted_addresses(_compiled(tmp_path, nf))
    assert len(violations) == 1
    assert "/scratch/agent/task_cache/1e/20ab/out/f.gbk" in violations[0]
    assert "cannot declare a bind of its own" in violations[0]


def test_foreign_literal_is_reported_too(tmp_path):
    nf = "workflow {\nChannel.of([[:], file('/shared/team_cache/aa/bb/out/f')])\n}\n"
    violations = check_emitted_addresses(_compiled(tmp_path, nf))
    assert len(violations) == 1
    assert "/shared/team_cache/aa/bb/out/f" in violations[0]


def _given_input_nf(*, consumed: bool, bind: str | None) -> str:
    process = (
        'process p01 {\nscript:\n"""\n'
        f'b1="{bind}"\n'
        'echo "--mount type=bind,source="\\$b1",target="\\$b1"" >.command.binds\n'
        '"""\n}\n'
    ) if bind else ""
    body = 'workflow {\n_X = (o.postIn([in("inputs/X6Ha2P0i", l)], ["X6Ha2P0i"]))[0]\n'
    if consumed:
        body += "_Y = (o.post(o.asStreams(p01(o.group('X', [_X], k, 1))), k, ['s']))[0]\n"
    return process + body + "}\n"


def _write_given_input(tmp_path: Path) -> None:
    (tmp_path / "inputs").mkdir()
    (tmp_path / "inputs" / "X6Ha2P0i").write_text(
        "/project/samples/sample_00/reads.fq\n", encoding="utf-8"
    )


def test_given_input_covered_by_a_declared_bind_passes(tmp_path):
    _write_given_input(tmp_path)
    nf = _given_input_nf(consumed=True, bind="/project/samples/sample_00")
    assert check_emitted_addresses(_compiled(tmp_path, nf)) == []


def test_given_input_with_no_bind_is_reported(tmp_path):
    _write_given_input(tmp_path)
    nf = _given_input_nf(consumed=True, bind=None)
    violations = check_emitted_addresses(_compiled(tmp_path, nf))
    assert len(violations) == 1
    assert "declared bind source" in violations[0]


def test_given_input_whose_consumer_was_cached_away_is_skipped(tmp_path):
    _write_given_input(tmp_path)
    nf = _given_input_nf(consumed=False, bind=None)
    assert check_emitted_addresses(_compiled(tmp_path, nf)) == []


HOST_HOME = Path("/scratch/agent")


def _container_map() -> PathMap:
    return PathMap(extern_home=HOST_HOME, task_key="K")


def test_host_rooted_missing_input_gets_its_local_spelling():
    pm = _container_map()
    host = HOST_HOME / "task_cache/1e/20ab/out/f.gbk"
    assert _as_home_rooted(pm, host) == HOME / "task_cache/1e/20ab/out/f.gbk"


def test_already_home_rooted_path_is_not_diagnosed():
    pm = _container_map()
    assert _as_home_rooted(pm, HOME / "task_cache/1e/20ab/out/f.gbk") is None


def test_foreign_path_is_not_diagnosed():
    pm = _container_map()
    assert _as_home_rooted(pm, Path("/project/refdb/tax.tsv")) is None


def test_relay_free_arm_is_never_diagnosed():
    pm = PathMap(extern_home=HOME, task_key="K")
    assert _as_home_rooted(pm, HOME / "task_cache/1e/20ab/out/f.gbk") is None


def test_host_local_arm_is_never_diagnosed(tmp_path):
    pm = PathMap(extern_home=tmp_path, task_key=tmp_path.name, host_local=True)
    assert _as_home_rooted(pm, tmp_path / "my_reads.fq") is None
