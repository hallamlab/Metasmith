from __future__ import annotations

import os
from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary

from tests.metasmith.cache._cache_harness import (
    build_samples_library,
    build_transform_library,
    build_types_library,
    build_workflow_task,
    capture_run,
    clear_trace,
    identity_transform_code,
)


TYPE_NAMES = ("seed", "mid", "out")


def _build_pipeline_task(root: Path):
    types_path = build_types_library(root, TYPE_NAMES)
    samples = build_samples_library(
        root, types_path, count=2, input_type="seed"
    )
    transforms = {
        "trA": identity_transform_code("trA", "seed", "mid"),
        "trB": identity_transform_code("trB", "mid", "out"),
    }
    tr_lib = build_transform_library(root / "tr", types_path, transforms)
    return build_workflow_task(
        samples,
        tr_lib,
        sample_type="seed",
        target_specs=[("out_target", {"out"})],
    )


def test_resubmitted_library_hits_cache(tmp_path, virtual_runtime):
    # The reuse leaf identity buys: the same files, untouched, planned again.
    # Leaf ids are the stat, so what carries across is a path whose mtime has
    # not moved -- not bytes that happen to match somewhere else.
    root = tmp_path / "a"
    types_path = build_types_library(root, TYPE_NAMES)
    samples = build_samples_library(root, types_path, count=2, input_type="seed")
    tr_lib = build_transform_library(
        root / "tr",
        types_path,
        {
            "trA": identity_transform_code("trA", "seed", "mid"),
            "trB": identity_transform_code("trB", "mid", "out"),
        },
    )

    def _plan():
        return build_workflow_task(
            DataInstanceLibrary.Load(samples.location),
            tr_lib,
            sample_type="seed",
            target_specs=[("out_target", {"out"})],
        )

    task_a = _plan()
    snap_a = capture_run(virtual_runtime, task_a)
    assert snap_a.executed_steps, "run A executed zero steps (bad fixture)"

    task_b = _plan()
    assert task_b.GetKey() == task_a.GetKey(), (
        "re-planning the same untouched library produced a different task key; "
        "leaf identity is not stable across runs, so reuse is impossible"
    )

    clear_trace(virtual_runtime)
    snap_b = capture_run(virtual_runtime, task_b)
    assert snap_b.executed_steps == (), (
        f"run B re-executed steps {snap_b.executed_steps}; a re-plan over "
        f"untouched inputs should be a full cache hit"
    )

    assert snap_b.result_fingerprints == snap_a.result_fingerprints, (
        "cross-run cache hit produced different output payloads than the "
        "original run — reentrancy must be results-preserving"
    )


def test_identical_bytes_elsewhere_do_not_collapse(tmp_path, virtual_runtime):
    # Deliberate, not a defect. Identity is the path and the mtime, so a second
    # copy of the same bytes under a different root is a different input. The
    # trade is one stat per leaf instead of a read of every byte.
    task_a = _build_pipeline_task(tmp_path / "a")
    task_b = _build_pipeline_task(tmp_path / "b")
    assert task_a.GetKey() != task_b.GetKey()


def test_perturbed_inputs_get_distinct_identity(tmp_path, virtual_runtime):
    types_path = build_types_library(tmp_path, TYPE_NAMES)

    def _leaf_id(payload: str, where: str) -> str:
        lib = DataInstanceLibrary(tmp_path / where)
        lib.Purge()
        lib.AddTypeLibrary(types_path, namespace="cf")
        (lib.location / "data.txt").write_text(payload, encoding="utf-8")
        lib.AddItem(Path("data.txt"), "cf::seed")
        return lib.Get(Path("data.txt")).instance_id

    id_original = _leaf_id("sample payload\n", "orig.xgdb")
    id_perturbed = _leaf_id("PERTURBED payload\n", "pert.xgdb")
    assert id_original != id_perturbed, (
        "two distinct inputs minted the same leaf id"
    )


def test_leaf_id_same_for_abs_and_rel_arguments(tmp_path, virtual_runtime):
    types_path = build_types_library(tmp_path, TYPE_NAMES)
    root = tmp_path / "root.xgdb"
    (root / "sub").mkdir(parents=True)
    (root / "sub" / "data.txt").write_text("payload\n", encoding="utf-8")

    def _leaf_id(use_absolute_arg: bool) -> str:
        lib = DataInstanceLibrary(root)
        lib.AddTypeLibrary(types_path, namespace="cf")
        arg = (root / "sub" / "data.txt") if use_absolute_arg else Path("sub/data.txt")
        lib.AddItem(arg, "cf::seed")
        return lib.Get(arg).instance_id

    assert _leaf_id(False) == _leaf_id(True), (
        "one file addressed relatively and absolutely minted two leaf ids; the "
        "identity is the resolved path, so the spelling must not matter"
    )


def test_leaf_random_optout_disables_cross_run(tmp_path, virtual_runtime):
    prev = os.environ.get("METASMITH_LEAF_RANDOM")
    os.environ["METASMITH_LEAF_RANDOM"] = "1"
    try:
        task_a = _build_pipeline_task(tmp_path / "a")
        task_b = _build_pipeline_task(tmp_path / "b")
    finally:
        if prev is None:
            os.environ.pop("METASMITH_LEAF_RANDOM", None)
        else:
            os.environ["METASMITH_LEAF_RANDOM"] = prev

    assert task_a.GetKey() != task_b.GetKey(), (
        "with METASMITH_LEAF_RANDOM=1, independent builds should mint "
        "distinct leaf ids and therefore distinct task keys"
    )
