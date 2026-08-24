"""The cache round trip, driven through the real Nextflow binary.

Everything under `tests/metasmith/cache` runs against `virtual_runtime`, which
is a second implementation of the publish path -- so a cache-hit re-run could
lose every product while that suite stayed green. These tests run the same two
runs through Nextflow in the dev image, sharing one agent home so the second
run reads the shards the first one wrote.

The transform lane is `-stub`: no tool runs and every process returns at once.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from metasmith.agents import CollectResults, PublishCachedProducts
from metasmith.caching.layout import default_cache_root
from metasmith.caching.promote import CANONICAL_OUTPUT_PREFIX, promote_run
from metasmith.constants import MODULE_PATH, AgentPaths
from metasmith.env import Runtime
from metasmith.models.workflow import NextflowGenContext, WorkflowTask
from metasmith.telemetry import TraceIndex

pytestmark = [pytest.mark.docker, pytest.mark.slow]

ORCHESTRATOR_SRC = MODULE_PATH / "nextflow_config/Orchestrator.groovy"


def _bind_root(work_dir: Path) -> Path:
    root = work_dir
    while root.parent != root and root.parent != Path("/tmp"):
        root = root.parent
    return root


def run_cached_stub(
    task: WorkflowTask,
    work_dir: Path,
    agent_home: Path,
    docker_image: str,
    *,
    before_promote=None,
    timeout: int = 300,
) -> Path:
    """One cached run. Returns the results directory.

    `agent_home` holds the cache and is meant to be shared across calls: that
    sharing is what makes the second run a hit.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    agent_home.mkdir(parents=True, exist_ok=True)
    cache_root = default_cache_root(agent_home)

    context = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=work_dir,
        external_work=work_dir,
        home_dir=agent_home,
        external_home=agent_home,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
        cache_root=cache_root,
    )
    task.PrepareNextflow(context)

    lib_dir = work_dir / "lib"
    lib_dir.mkdir(exist_ok=True)
    shutil.copy(ORCHESTRATOR_SRC, lib_dir / "Orchestrator.groovy")

    bind = _bind_root(work_dir)
    result = subprocess.run(
        [
            "docker", "run", "--rm",
            "-v", f"{bind}:{bind}",
            "-w", str(work_dir),
            docker_image,
            "nextflow", "run", AgentPaths.NXF_WORKFLOW,
            "-stub",
            "-lib", "./lib",
            "-ansi-log", "false",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert result.returncode == 0, (
        f"Nextflow run failed:\nSTDOUT:\n{result.stdout[-4000:]}\n"
        f"STDERR:\n{(result.stderr or '')[-4000:]}"
    )
    subprocess.run(
        ["docker", "run", "--rm", "-v", f"{bind}:{bind}",
         docker_image, "chmod", "-R", "a+rw", str(bind)],
        capture_output=True, timeout=120,
    )

    if before_promote is not None:
        before_promote(work_dir, cache_root)

    promote_run(workspace=work_dir, cache_root=cache_root)
    # What the driver does between nextflow exiting and the results being
    # compiled; nextflow itself will not publish out of a cache shard.
    PublishCachedProducts(work_dir, work_dir / "results")
    output = CollectResults(
        task=task,
        output_path=work_dir / "results",
        inputs_dir=work_dir / "inputs",
    )
    output.Save()
    return work_dir / "results"


def _statuses(work_dir: Path) -> set[str]:
    trace = TraceIndex.read(work_dir / "_metasmith" / "trace.jsonl")
    return {e.status for e in trace.events if e.status}


def _results_tree(results: Path) -> dict[str, bytes]:
    """Every published byte, keyed by path relative to `results`.

    `_metadata` and `given.csv` carry per-run identity and are excluded.
    """
    tree: dict[str, bytes] = {}
    for fp in sorted(results.rglob("*")):
        if not fp.is_file():
            continue
        rel = fp.relative_to(results)
        if rel.parts and rel.parts[0] == "_metadata":
            continue
        if str(rel) == "given.csv":
            continue
        tree[str(rel)] = fp.read_bytes()
    return tree


def _make_one_product_a_directory(work_dir: Path, cache_root: Path) -> list[str]:
    """Turn one staged product into a directory holding two files.

    `publishDir mode: 'link'` stages a directory product as a real directory
    whose leaves are hard links (verified against Nextflow 26.04.1) -- but the
    stub lane only `touch`es its mock outputs, so it cannot produce one. Doing
    the swap on the staged product keeps the name the slot matches on and
    leaves promotion, the shard, the hit and the publish entirely real.
    """
    converted: list[str] = []
    for tmp in sorted(cache_root.glob("*.tmp")):
        for fp in sorted(tmp.iterdir()):
            if not fp.is_file() or not CANONICAL_OUTPUT_PREFIX.match(fp.name):
                continue
            fp.unlink()
            fp.mkdir()
            (fp / "a.txt").write_text("alpha\n")
            (fp / "b.txt").write_text("beta\n")
            converted.append(fp.name)
            break
    assert converted, "no staged product to convert; the run cached nothing"
    return converted


class TestCacheHitPublishesResults:
    def test_second_run_is_all_hits(
        self, simple_workflow_task, tmp_path, docker_image
    ):
        home = tmp_path / "agent_home"
        run1 = tmp_path / "run1"
        run2 = tmp_path / "run2"

        run_cached_stub(simple_workflow_task, run1, home, docker_image)
        assert "hit" not in _statuses(run1), "cold cache reported a hit"

        run_cached_stub(simple_workflow_task, run2, home, docker_image)
        assert _statuses(run2) == {"hit"}, (
            f"second run was not fully served from cache: {_statuses(run2)}"
        )

    def test_cached_rerun_publishes_the_same_results(
        self, simple_workflow_task, tmp_path, docker_image
    ):
        home = tmp_path / "agent_home"
        first = run_cached_stub(
            simple_workflow_task, tmp_path / "run1", home, docker_image
        )
        second = run_cached_stub(
            simple_workflow_task, tmp_path / "run2", home, docker_image
        )
        assert _results_tree(second) == _results_tree(first), (
            "a fully cached re-run did not reproduce the first run's results"
        )


class TestDirectoryProducts:
    def test_a_directory_product_reaches_the_shard(
        self, simple_workflow_task, tmp_path, docker_image
    ):
        home = tmp_path / "agent_home"
        names: list[str] = []
        run_cached_stub(
            simple_workflow_task, tmp_path / "run1", home, docker_image,
            before_promote=lambda w, c: names.extend(
                _make_one_product_a_directory(w, c)
            ),
        )
        cache_root = default_cache_root(home)
        promoted = [
            d
            for name in names
            for d in cache_root.rglob(f"out/{name}")
        ]
        assert promoted, (
            f"no shard holds {names}; a directory product was not promoted"
        )
        for d in promoted:
            assert d.is_dir(), f"{d} landed in the shard as a file"
            assert (d / "a.txt").read_text() == "alpha\n"
            assert (d / "b.txt").read_text() == "beta\n"

    def test_a_directory_product_is_served_and_published(
        self, simple_workflow_task, tmp_path, docker_image
    ):
        home = tmp_path / "agent_home"
        names: list[str] = []
        run_cached_stub(
            simple_workflow_task, tmp_path / "run1", home, docker_image,
            before_promote=lambda w, c: names.extend(
                _make_one_product_a_directory(w, c)
            ),
        )
        run2 = tmp_path / "run2"
        second = run_cached_stub(
            simple_workflow_task, run2, home, docker_image
        )
        assert _statuses(run2) == {"hit"}, (
            f"a shard holding a directory did not serve: {_statuses(run2)}"
        )
        published = [
            p for p in second.rglob("*") if p.is_dir() and p.name in set(names)
        ]
        assert published, (
            f"none of {names} reached {second}; a cached directory product "
            "was not published"
        )
        for d in published:
            assert (d / "a.txt").read_text() == "alpha\n"
            assert (d / "b.txt").read_text() == "beta\n"
