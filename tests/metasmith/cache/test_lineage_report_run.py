"""The lineage report a real driver writes, cold and from shards, and a post-run step that raises."""

from __future__ import annotations

import csv
from pathlib import Path

from metasmith.agents.lineage_report import LINEAGE_CSV, PARENTS_CSV
from metasmith.constants import AgentPaths

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.cache.fixtures.cache_fixtures import parallel_then_group

LOG_DIR = Path("_metasmith/logs.virtual")


def _read(workspace: Path, name: str) -> tuple[list[str], list[list[str]]]:
    header, *rows = list(csv.reader((workspace / LOG_DIR / name).open()))
    return header, rows


def _column(header: list[str], rows: list[list[str]], suffix: str) -> set[str]:
    [i] = [i for i, c in enumerate(header) if c.endswith(suffix)]
    return {r[i] for r in rows}


def test_a_merge_leaves_one_row_per_merged_member(tmp_path, virtual_runtime):
    run = capture_run(virtual_runtime, parallel_then_group.build_task(tmp_path))

    header, rows = _read(run.workspace, LINEAGE_CSV)
    assert len(rows) == 3, rows
    assert len(_column(header, rows, "seed")) == 3
    assert len(_column(header, rows, "step_a")) == 3
    assert len(_column(header, rows, "step_b")) == 1
    assert len(_column(header, rows, "step_c")) == 1
    assert all(Path(cell).exists() for row in rows for cell in row if cell)

    _, edges = _read(run.workspace, PARENTS_CSV)
    pairs = {(c.split("::")[-1], p.split("::")[-1]) for c, p in edges}
    assert {("step_a", "seed"), ("step_b", "step_a"), ("step_b", "root"), ("step_c", "step_b")} <= pairs


def test_a_warm_run_points_its_products_into_shards(tmp_path, virtual_runtime):
    task = parallel_then_group.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    clear_trace(virtual_runtime)
    warm = capture_run(virtual_runtime, task)

    header, rows = _read(warm.workspace, LINEAGE_CSV)
    assert len(rows) == 3, rows
    cache_root = virtual_runtime.home / "task_cache"
    for suffix in ("step_a", "step_b", "step_c"):
        for cell in _column(header, rows, suffix):
            assert Path(cell).is_relative_to(cache_root), f"{suffix} served from {cell}"


def test_a_failed_collect_still_ends_on_one_failed_sentinel(tmp_path, virtual_runtime, monkeypatch):
    def _raise(**_):
        raise RuntimeError("collect exploded")

    monkeypatch.setattr("metasmith.agents.runner.CollectResults", _raise)
    run = capture_run(virtual_runtime, parallel_then_group.build_task(tmp_path))

    log = (run.workspace / LOG_DIR / AgentPaths.MAIN_LOG_FILE).read_text()
    failed = [l for l in log.splitlines() if AgentPaths.RUN_FAILED_SENTINEL in l]
    assert len(failed) == 1 and "collect results" in failed[0], failed
    assert AgentPaths.RUN_DONE_SENTINEL not in log
    assert (run.workspace / LOG_DIR / LINEAGE_CSV).is_file()
