"""The task metadata file has to survive `scratch`.

Every task echoes its on-channel lineage index into `.command.metadata` in
its current directory, and `promote._collect_output_indexes` reads it back
out of the work tree afterwards. Under Nextflow's `scratch` directive the
current directory is node-local and thrown away at task exit: Nextflow
copies back the declared outputs and `.command.{out,err,trace}`, and nothing
else. `.command.metadata` is neither, so it dies with the scratch dir and
every shard the run promotes is written without an index.

That is not a hypothetical — the reporting agent home holds 88 tasks across
12 runs and zero metadata files. `scratch` is on for every SLURM profile
(`nextflow_config/slurm.nf`), so the whole cluster path is the broken one.

The tests below splice the *shipped* generated script into a minimal
process. The metadata lines and the stub's output-naming expression are
lifted verbatim out of a `workflow.nf` produced by the real compiler, so
the fix is exercised where it lives rather than in a hand-written mimic;
what the tests own is only the surrounding process and the `scratch`
directive the cluster config would have set.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step
from tests.metasmith.e2e.docker.test_orchestrator_exec import NxfTestRunner

pytestmark = [pytest.mark.docker, pytest.mark.nextflow, pytest.mark.slow]

# The step whose generated script is spliced below. Any step would do; the
# metadata section is identical for all of them.
_STEP_ORDER = 1
_STEP_META = f"workflow.step_{_STEP_ORDER}.meta"

# One index entry, so the payload's `entries[0]` lines up with the `1-...`
# batch member the stub writes.
_INDEX = '[["seed": ["1e20aaaa"]]]'


class _Generated:
    """The pieces of one compiler-generated process, ready to re-host.

    `script` stops at the last line that touches `.command.metadata`: what
    follows it in the real process is the bootstrap call that runs the
    transform, which needs an agent home this test does not have. Stopping
    there keeps every line that is about the metadata file — including,
    once the fix lands, the copy that puts it back in the work directory.
    """

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
        # The stub names its outputs from the index it was handed; reusing
        # the expression keeps the filename this test produces the one the
        # rest of the cache would expect.
        self.hash_def = next(
            l for l in stub.group(1).splitlines() if l.startswith("def hash")
        )
        self.touch = next(
            l for l in stub.group(2).splitlines() if l.startswith("touch")
        )


def _stage(root: Path):
    """Compile `linear_3step` into `root/staged` and return (task, workspace).

    `root` is what gets mounted into the container, so everything the run
    touches — the staged workspace, the Nextflow work tree, the cache —
    lives under it.
    """
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
    """A one-process workflow around the generated metadata section."""
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
    """Run `nf` and return the single task's work directory on the host.

    Nextflow runs as root in the container, so everything it writes into the
    bind mount comes back root-owned and the host-side promote cannot touch
    it. Hand it back before returning.
    """
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
    """A compiled workspace, its generated process, and a runner over both."""
    root = tmp_path / "run"
    task, workspace = _stage(root)
    gen = _Generated((workspace / "workflow.nf").read_text())
    runner = NxfTestRunner(root, docker_image)
    return task, workspace, gen, runner


def test_the_metadata_reaches_the_work_dir_under_scratch(staged):
    """F4a: with `scratch true`, `.command.metadata` is in the work dir.

    Pre-fix it is simply absent — the task wrote it into the scratch
    directory and Nextflow copied back only the declared output. This arm
    has never once passed on a cluster.
    """
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
    """F4b: without `scratch`, the file is there and nothing complains.

    This passes before and after. It exists to catch the one way the fix
    can break a working configuration: copying the metadata onto itself
    when the work directory already *is* the current directory.
    """
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
    """F4c: the reported symptom — a restart that re-runs everything.

    Run one executes under `scratch` and is promoted; run two probes the
    cache. Pre-fix the promote finds no metadata, writes the shard without
    an index, and the probe demotes it to a miss (`cache_decisions`
    requires every cached output to carry ancestry, since replaying an
    empty index stops the run). So the step recomputes every time, which is
    what priced a restart at days.
    """
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
