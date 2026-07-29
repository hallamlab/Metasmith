"""The address contract every codegen producer has to satisfy.

Whatever codegen writes into the workflow graph is read back inside the
per-step bootstrap container, which mounts the task work dir, the agent home
at `HOME_ROOT`, and whatever `.command.binds` declares. Four producers write
addresses today; a fifth added later must be caught here rather than on
someone's run, which is what `check_emitted_addresses` is for.

The positive direction (real fixtures stage clean) is asserted in
`tests/e2e/virtual/test_contract_smoke.py`; this file owns the negative one,
since a checker nothing ever trips is indistinguishable from no checker.

The second half is the runtime diagnostic: when an address does slip through,
`bootstrap._as_home_rooted` turns "missing input" into the actual diagnosis.
Both enforcement points guard one contract, so they are pinned together.
"""

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
    """The ppanggolin failure, caught at compile time instead of run time.

    A cache-hit channel emitted with the host spelling of the agent home:
    the head process stages it happily (it has an identity bind the per-step
    container does not) and the consumer then reports a present file as
    missing.
    """
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
    """A shared cache root outside the agent home has the same problem.

    `cache_root` is a settable field. Point it at a team-wide cache — the
    configuration the cache feature exists to enable — and the address goes
    on the wire with nothing mounted anywhere near it. Naming it here so the
    day someone configures one, the checker says why.
    """
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
    """Foreign given-inputs are legitimate — `.command.binds` serves them."""
    _write_given_input(tmp_path)
    nf = _given_input_nf(consumed=True, bind="/project/samples/sample_00")
    assert check_emitted_addresses(_compiled(tmp_path, nf)) == []


def test_given_input_with_no_bind_is_reported(tmp_path):
    """The same row without a bind is a file the step cannot open."""
    _write_given_input(tmp_path)
    nf = _given_input_nf(consumed=True, bind=None)
    violations = check_emitted_addresses(_compiled(tmp_path, nf))
    assert len(violations) == 1
    assert "declared bind source" in violations[0]


def test_given_input_whose_consumer_was_cached_away_is_skipped(tmp_path):
    """A channel registered for lineage but never staged needs no bind.

    When the only step consuming a given input is a cache hit, codegen still
    emits `o.postIn` — the orchestrator needs the lineage registration — but
    no process ever stages those files, so there is nothing to bind and
    nothing to flag. Found by running the checker against real staged output
    rather than by reading the emitter.
    """
    _write_given_input(tmp_path)
    nf = _given_input_nf(consumed=False, bind=None)
    assert check_emitted_addresses(_compiled(tmp_path, nf)) == []


# ---------------------------------------------------------------------------
# The runtime half: naming the cause at the frame that already detected it


HOST_HOME = Path("/scratch/agent")


def _container_map() -> PathMap:
    return PathMap(extern_home=HOST_HOME, task_key="K")


def test_host_rooted_missing_input_gets_its_local_spelling():
    """The diagnosis the bootstrap's existence check already had the facts for."""
    pm = _container_map()
    host = HOST_HOME / "task_cache/1e/20ab/out/f.gbk"
    assert _as_home_rooted(pm, host) == HOME / "task_cache/1e/20ab/out/f.gbk"


def test_already_home_rooted_path_is_not_diagnosed():
    """A missing HOME_ROOT path is a missing file, not a coordinate mistake."""
    pm = _container_map()
    assert _as_home_rooted(pm, HOME / "task_cache/1e/20ab/out/f.gbk") is None


def test_foreign_path_is_not_diagnosed():
    """`.command.binds` legitimately identity-binds paths outside the home."""
    pm = _container_map()
    assert _as_home_rooted(pm, Path("/project/refdb/tax.tsv")) is None


def test_relay_free_arm_is_never_diagnosed():
    """No boundary, no translation — the two roots are one directory."""
    pm = PathMap(extern_home=HOME, task_key="K")
    assert _as_home_rooted(pm, HOME / "task_cache/1e/20ab/out/f.gbk") is None


def test_host_local_arm_is_never_diagnosed(tmp_path):
    """`metasmith run`'s agent home can be the user's cwd.

    Telling them their own input "should have been" a `/msm_home` path would
    be advice to introduce the very bug this message exists to name.
    """
    pm = PathMap(extern_home=tmp_path, task_key=tmp_path.name, host_local=True)
    assert _as_home_rooted(pm, tmp_path / "my_reads.fq") is None
