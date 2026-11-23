"""A run keeps its targets and deletes the rest, and a rerun remakes nothing it can skip.

The generated workflow runs on the host's Nextflow against the real
Orchestrator, member-key helper and promotion. Only the transform is stood in
for: `lib/msm_bootstrap` writes each member's products under the names
bootstrap gives them and promotes them, the way a task does after its
protocol. The run sequence is the driver's: the probe pass when the store has
lost a product, `plan_skips`, the real pass, `record_run`.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from metasmith.agents import CollectResults
from metasmith.caching import footprint
from metasmith.caching.invocation import read_manifest, shard_state
from metasmith.caching.layout import default_cache_root
from metasmith.caching.promote import record_run
from metasmith.constants import MODULE_PATH, AgentPaths
from metasmith.env import Runtime
from metasmith.models.workflow import NextflowGenContext, WorkflowTask
from metasmith.testing import mock_transforms as mt

from tests.metasmith.e2e.nextflow.harness import host_nextflow
from tests.metasmith.flow.conftest import (
    _build_samples_lib, _build_transform_lib, _build_type_lib, _generate_plan,
)

pytestmark = pytest.mark.xdist_group("footprint")

CHAIN = ["assembly", "bam", "branch_a", "branch_b", "merged"]
REPO = Path(__file__).resolve().parents[4]

BOOTSTRAP = r'''
import json, os, sys
from pathlib import Path
from metasmith.caching.layout import default_cache_root
from metasmith.caching.promote import StepCacheMeta, promote_members
from metasmith.models.lineage import LinPayload
from metasmith.models.workflow.payload import member_token

home = Path(sys.argv[1])
raw = {}
for line in Path(".command.metadata").read_text().splitlines():
    head, _, rest = line.partition(" ")
    raw[head] = rest
order = int(sys.argv[3])
meta = StepCacheMeta.from_raw(order, raw)
entries = LinPayload.from_json(raw["lin"]).entries
with open(home / "ran.log", "a") as f:
    for e in entries:
        f.write(f"{meta.step_name} {e.get('KEY')}\n")
if meta.step_name in (home / "fail").read_text().split():
    sys.exit(1)
fan = 2 if meta.step_name in (home / "fan").read_text().split() else 1
for i, e in enumerate(entries):
    for sf in meta.slot_files:
        for j in range(fan):
            name = f"{i+1}-{j+1}-{sf['branch_idx']+1}.{member_token(e)}-{sf['dtype_key']}{sf['ext']}"
            Path(name).write_text(f"{meta.step_name} {e.get('KEY')} {j}\n")
promote_members(
    cwd=Path.cwd(), entries=entries, meta=meta,
    cache_root=default_cache_root(home), successes=[True] * len(entries),
)
Path(".command.success").touch()
'''


@dataclass
class Run:
    returncode: int
    output: str
    ran: list[str]
    processes: list[str]
    results: Path
    task: WorkflowTask


class Agent:
    """One agent home shared by every run, so a later run sees what an earlier one cached."""

    def __init__(self, root: Path):
        if host_nextflow() is None:
            pytest.skip("no nextflow on PATH or beside this python; run under `mamba run -n msm`")
        self.root = root
        self.home = root / "home"
        self.cache_root = default_cache_root(self.home)
        (self.home / "lib").mkdir(parents=True, exist_ok=True)
        (self.home / "lib" / "fake_bootstrap.py").write_text(BOOTSTRAP)
        boot = self.home / "lib" / "msm_bootstrap"
        boot.write_text(f'#!/bin/bash\nexec {sys.executable} {self.home}/lib/fake_bootstrap.py {self.home} "$@"\n')
        boot.chmod(0o755)
        self.fail()
        self.fan()

    def fail(self, *steps: str) -> None:
        (self.home / "fail").write_text(" ".join(steps))

    def fan(self, *steps: str) -> None:
        # Each member of `steps` writes two files per output, as a glob output can.
        (self.home / "fan").write_text(" ".join(steps))

    def plan(self, n_steps: int, n_samples: int = 2) -> WorkflowTask:
        types = _build_type_lib(self.root / "types.yml")
        samples = _build_samples_lib(self.root, types, n_samples=n_samples, dtype=CHAIN[0])
        transforms = {}
        for i in range(n_steps):
            transforms.update(mt.identity_transform(f"mock::{CHAIN[i]}", f"mock::{CHAIN[i+1]}"))
        tr = _build_transform_lib(self.root / "tr", types, transforms)
        plan = _generate_plan(
            samples, tr, sample_dtype=CHAIN[0],
            target_props=[{CHAIN[n_steps]}], target_names=[CHAIN[n_steps]],
        )
        return WorkflowTask(ok=True, plan=plan, data_libraries=[samples], transform_libraries=[tr])

    def run(self, n_steps: int, name: str, keep: bool = False) -> Run:
        # Planned afresh every run, as a stage does: codegen stamps slot ids
        # onto the plan it is handed, and a plan stamped once does not stamp
        # to the same ids again.
        task = self.plan(n_steps)
        task.plan.keep_intermediates = keep
        ws = self.root / name
        ws.mkdir(parents=True, exist_ok=True)
        task.PrepareNextflow(NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW, work_dir=ws, external_work=ws,
            home_dir=self.home, external_home=self.home, runtime=Runtime.DOCKER,
            resources_file=AgentPaths.NXF_RES, cache_root=self.cache_root,
        ))
        (ws / "lib").mkdir(exist_ok=True)
        shutil.copy(MODULE_PATH / "nextflow_config/Orchestrator.groovy", ws / "lib")
        (self.home / "ran.log").unlink(missing_ok=True)
        (ws / "trace.tsv").unlink(missing_ok=True)
        (ws / footprint.DELETED_LOG).unlink(missing_ok=True)

        footprint.reset(ws)
        if not task.plan.keep_intermediates and footprint.has_ghosts(self.cache_root):
            probe = self._nextflow(
                ws, "--msm_probe", "true", "-w", str(ws / "nxf_probe"),
                env={"NXF_CACHE_DIR": str(ws / "nxf_probe" / ".nextflow")},
            )
            assert probe.returncode == 0, probe.stdout[-3000:] + probe.stderr[-3000:]
            footprint.plan_skips(ws, self.cache_root)
        nxf = self._nextflow(ws, "-w", str(ws / "nxf_work"), "-with-trace", "trace.tsv", "-resume")
        record_run(workspace=ws, cache_root=self.cache_root)
        results = ws / "results"
        CollectResults(task=task, output_path=results, inputs_dir=ws / "inputs").Save()
        ran_log = self.home / "ran.log"
        trace = ws / "trace.tsv"
        rows = trace.read_text().splitlines()[1:] if trace.exists() else []
        return Run(
            returncode=nxf.returncode,
            output=nxf.stdout[-4000:] + nxf.stderr[-4000:],
            ran=sorted(l.split()[0] for l in ran_log.read_text().splitlines()) if ran_log.exists() else [],
            processes=sorted(r.split("\t")[3].split(" ")[0] for r in rows if r.strip()),
            results=results,
            task=task,
        )

    def _nextflow(self, ws: Path, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
        env = {
            **os.environ, **(env or {}), "NXF_ANSI_LOG": "false",
            "PATH": f"{Path(sys.executable).parent}:{os.environ.get('PATH', '')}",
            "PYTHONPATH": f"{REPO / 'src'}:{REPO}",
        }
        return subprocess.run(
            [str(host_nextflow()), "run", AgentPaths.NXF_WORKFLOW, "-lib", "./lib",
             "-c", AgentPaths.NXF_RES, "-ansi-log", "false", "--hostName", "test",
             "--output", "results", *args],
            cwd=ws, env=env, capture_output=True, text=True, timeout=300,
        )

    def shards(self, step: str) -> dict[str, str]:
        """Each shard of `step` by key, as `hit`, `ghost` or `miss`."""
        out = {}
        for m in self.cache_root.glob("*/*/manifest.cbor"):
            manifest = read_manifest(m.parent) or {}
            if manifest.get("step_name") == step:
                key = m.parent.parent.name + m.parent.name
                out[key] = shard_state(self.cache_root, bytes.fromhex(key))[0]
        return out


def _states(agent: Agent, *steps: str) -> dict[str, set[str]]:
    return {s: set(agent.shards(f"identity_{s}").values()) for s in steps}


def _published(results: Path) -> set[str]:
    return {p.name for p in results.iterdir() if not p.name.startswith(("_", "given"))}


def _work_products(ws: Path, dtype_key: str) -> list[Path]:
    return [p for p in (ws / "nxf_work").rglob(f"*-{dtype_key}*") if p.is_file()]


@pytest.fixture
def agent(tmp_path) -> Agent:
    return Agent(tmp_path)


def test_a_run_keeps_only_its_target(agent):
    run = agent.run(3, "ws")
    assert run.returncode == 0, run.output
    assert _states(agent, "bam", "branch_a", "branch_b") == {
        "bam": {"ghost"}, "branch_a": {"ghost"}, "branch_b": {"hit"},
    }
    assert _published(run.results) == {"branch_b"}
    made = {i.dtype_name: i.dtype.key for st in run.task.plan.steps for g in st.produces for i in g}
    for name in ("mock::bam", "mock::branch_a"):
        assert _work_products(agent.root / "ws", made[name]) == [], f"{name} stayed in the work dir"
    assert _work_products(agent.root / "ws", made["mock::branch_b"])
    deleted = [json.loads(l) for l in (agent.root / "ws" / footprint.DELETED_LOG).read_text().splitlines()]
    assert len(deleted) == 4, deleted


def test_keep_intermediates_deletes_nothing_and_publishes_everything(agent):
    run = agent.run(3, "ws", keep=True)
    assert run.returncode == 0, run.output
    assert _states(agent, "bam", "branch_a", "branch_b") == {
        "bam": {"hit"}, "branch_a": {"hit"}, "branch_b": {"hit"},
    }
    assert len(_published(run.results)) == 3, _published(run.results)


def test_a_failed_reader_keeps_its_inputs_and_the_rerun_resumes_there(agent):
    agent.fail("identity_branch_b")
    first = agent.run(3, "ws")
    assert _states(agent, "bam", "branch_a", "branch_b") == {
        "bam": {"ghost"}, "branch_a": {"hit"}, "branch_b": set(),
    }, first.output

    agent.fail()
    second = agent.run(3, "ws")
    assert second.returncode == 0, second.output
    assert second.ran == ["identity_branch_b"] * 2, second.ran
    assert _states(agent, "bam", "branch_a", "branch_b") == {
        "bam": {"ghost"}, "branch_a": {"ghost"}, "branch_b": {"hit"},
    }
    assert _published(second.results) == {"branch_b"}


@pytest.mark.parametrize("fan", [False, True], ids=["one_file", "runtime_fan_out"])
def test_a_full_rerun_runs_nothing(agent, fan):
    # Fanned out, each of two samples' bam is two files and every step below
    # runs once per file: a skipped member must still deliver every file its
    # manifest names, or its readers form different members and miss.
    if fan:
        agent.fan("identity_bam")
    first = agent.run(3, "ws")
    assert first.returncode == 0, first.output
    assert len(first.ran) == (10 if fan else 6), first.ran
    again = agent.run(3, "ws")
    assert again.returncode == 0, again.output
    assert again.ran == [], again.ran
    assert all(p.endswith("_cached") for p in again.processes), again.processes
    assert _published(again.results) == {"branch_b"}
    assert _states(agent, "branch_b") == {"branch_b": {"hit"}}


def test_a_new_target_on_kept_products_runs_only_the_new_step(agent):
    assert agent.run(3, "ws3").returncode == 0
    run = agent.run(4, "ws4")
    assert run.returncode == 0, run.output
    assert run.ran == ["identity_merged"] * 2, run.ran
    assert _published(run.results) == {"merged"}
