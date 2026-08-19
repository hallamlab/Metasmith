from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step
from tests.metasmith.e2e.docker.test_orchestrator_exec import NxfTestRunner

pytestmark = [pytest.mark.docker, pytest.mark.nextflow, pytest.mark.slow]

_STEP_ORDER = 1
_STEP_META = f"workflow.step_{_STEP_ORDER}.meta"

_INDEX = '[["seed": ["1e20aaaa"]]]'


class _Generated:
    def __init__(self, workflow_nf: str):
        block = re.search(
            r"^process (\w+) \{\n(.*?)\n\}\n", workflow_nf, re.S | re.M
        )
        assert block, f"no process in generated workflow:\n{workflow_nf}"
        body = block.group(2)

        out = re.search(r"^output:\n(.*)$", body, re.M)
        script = re.search(r"^script:\n\"\"\"\n(.*?)\n\"\"\"$", body, re.S | re.M)
        stub = re.search(
            r"^stub:\n(.*?)\n\"\"\"\n(.*?)\n\"\"\"$", body, re.S | re.M
        )
        assert out and script and stub, f"unfamiliar process shape:\n{body}"

        self.output_decl = out.group(1)
        lines = script.group(1).splitlines()
        last = max(i for i, l in enumerate(lines) if ".command.metadata" in l)
        self.script = lines[: last + 1]
        self.hash_def = next(
            l for l in stub.group(1).splitlines() if l.startswith("def hash")
        )
        self.touch = next(
            l for l in stub.group(2).splitlines() if l.startswith("touch")
        )


def _stage(root: Path):
    from metasmith.constants import AgentPaths
    from metasmith.env import Runtime
    from metasmith.models.workflow import NextflowGenContext

    workspace = root / "staged"
    workspace.mkdir(parents=True)
    cache_root = root / "task_cache"
    cache_root.mkdir()
    task = linear_3step.build_task(root / "fixture")
    task.PrepareNextflow(
        NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=workspace,
            external_work=workspace,
            home_dir=AgentPaths.HOME_ROOT,
            external_home=AgentPaths.HOME_ROOT,
            runtime=Runtime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
            cache_root=cache_root,
        )
    )
    return task, workspace


def _nf(gen: _Generated, *, scratch: bool, publish_to: str | None) -> str:
    directives = []
    if scratch:
        directives.append("\tscratch true")
    if publish_to is not None:
        directives.append(
            f"\tpublishDir \"{publish_to}\", mode: 'copy', "
            "overwrite: true, pattern: '*'"
        )
    return "\n".join([
        f'params.workspace = "/ws/staged"',
        "",
        "process meta_probe {",
        *directives,
        "input:",
        "\ttuple val(index),path(_01)",
        "output:",
        gen.output_decl,
        "script:",
        gen.hash_def,
        '"""',
        *gen.script,
        gen.touch,
        '"""',
        "}",
        "",
        "workflow {",
        f'    ch = Channel.fromList([[{_INDEX}, file("${{projectDir}}/seed.txt")]])',
        "    meta_probe(ch)",
        "}",
        "",
    ])


def _run(runner: NxfTestRunner, nf: str, work_dir: str) -> Path:
    (runner.work_dir / "seed.txt").write_text("seed")
    result = runner.run(nf, extra_args=["-work-dir", work_dir])
    subprocess.run(
        [
            "docker", "run", "--rm", "-v", f"{runner.work_dir}:/ws",
            runner.docker_image,
            "chown", "-R", f"{os.getuid()}:{os.getgid()}", "/ws",
        ],
        capture_output=True,
    )
    NxfTestRunner.assert_nxf_ok(result)
    task_dirs = [
        p.parent for p in runner.work_dir.rglob(".command.sh")
    ]
    assert len(task_dirs) == 1, (
        f"expected exactly one task dir, got {task_dirs}\n{result.stdout}"
    )
    return task_dirs[0]


@pytest.fixture
def staged(tmp_path, docker_image):
    root = tmp_path / "run"
    task, workspace = _stage(root)
    gen = _Generated((workspace / "workflow.nf").read_text())
    runner = NxfTestRunner(root, docker_image)
    return task, workspace, gen, runner


def test_the_metadata_reaches_the_work_dir_under_scratch(staged):
    _task, _ws, gen, runner = staged
    task_dir = _run(
        runner, _nf(gen, scratch=True, publish_to=None), "/ws/staged/nxf_work"
    )

    meta = task_dir / ".command.metadata"
    present = sorted(p.name for p in task_dir.iterdir())
    assert meta.exists(), (
        "the task's metadata did not survive its scratch directory, so the "
        "index every output travelled with is unrecoverable and the shard "
        f"promotes without one. work dir holds: {present}"
    )
    body = meta.read_text()
    assert '"seed":["1e20aaaa"]' in body, (
        f"the metadata arrived but not the index it was written with:\n{body}"
    )
    assert f"cache_key " in body, (
        f"the step meta was not folded into the copy:\n{body}"
    )


def test_scratch_off_is_unchanged_and_silent(staged):
    _task, _ws, gen, runner = staged
    task_dir = _run(
        runner, _nf(gen, scratch=False, publish_to=None), "/ws/staged/nxf_work"
    )

    assert (task_dir / ".command.metadata").exists()
    err = (task_dir / ".command.err").read_text()
    assert err.strip() == "", (
        f"the copy step wrote to the task's stderr:\n{err}"
    )


def test_a_second_run_hits_instead_of_demoting(staged):
    from metasmith.caching.promote import promote_run
    from metasmith.models.workflow.cache_decisions import compute_cache_decisions

    task, workspace, gen, runner = staged
    cache_root = workspace.parent / "task_cache"
    key_hex = next(
        l.split(" ", 1)[1].strip()
        for l in (workspace / _STEP_META).read_text().splitlines()
        if l.startswith("cache_key ")
    )

    _run(
        runner,
        _nf(gen, scratch=True, publish_to=f"/ws/task_cache/{key_hex}.tmp"),
        "/ws/staged/nxf_work",
    )

    log: list = []
    summary = promote_run(workspace=workspace, cache_root=cache_root, log=log)
    assert key_hex in summary["promoted"], (
        f"run one promoted nothing for step {_STEP_ORDER}: {summary}, {log}"
    )

    decisions = compute_cache_decisions(task, _probe_context(workspace, cache_root))
    first = min(decisions)
    assert decisions[first]["hit"], (
        "the second run demoted the shard the first one promoted; the "
        "metadata never reached the work directory, so the shard carries no "
        f"ancestry and cannot be replayed. promote log: {log}"
    )
    assert decisions[first]["out_indexes"], (
        "the shard replayed with no index, which stops the run at the first "
        "descendant group"
    )


def _probe_context(workspace: Path, cache_root: Path):
    from metasmith.constants import AgentPaths
    from metasmith.env import Runtime
    from metasmith.models.workflow import NextflowGenContext

    return NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=workspace,
        external_work=workspace,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=AgentPaths.HOME_ROOT,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
        cache_root=cache_root,
    )
