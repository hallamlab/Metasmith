"""Cross-run reentrancy axis (R1/R2).

The reentrancy contract: a SECOND, independent invocation of the same
pipeline on the same input files automatically reuses the first run's
cached results — without a manual `metasmith data import-library` bridge
and without reusing the first run's library object.

This is the axis that the leaf-identity model used to *forbid*: pre-R1,
every `AddItem` minted a random `multihash(blake3, uuid4 || time_ns)`
leaf id, so two independently-built libraries over the same bytes minted
different leaf ids → different cache_keys → cache miss → recompute (see
the historical note in `test_cross_workflow.py`). R1 content-addresses a
present leaf file (`multihash(blake3(file_bytes))`), so identical input
bytes mint identical leaf ids, the whole cache_key chain matches, and the
second run resumes from the cache.

Distinct from:
  * `test_cross_task.py`  — reuse *within* one plan (shared upstream).
  * `test_cross_workflow.py` — reuse across plans via the origin column.
  * `test_hit_miss.py`    — reuse across reruns of the *same task object*.
Here the second run is a genuinely fresh library object at a different
filesystem location.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.cache._cache_harness import (
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
    """Build seed -> trA(mid) -> trB(out) from a library rooted at `root`.

    Every input file carries deterministic content, so two calls with
    different `root` dirs produce byte-identical inputs — the precondition
    for cross-run reuse.
    """
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


def test_fresh_library_same_inputs_hits_cache(tmp_path, virtual_runtime):
    """R2: an independent run on the same input bytes resumes from cache.

    Run A builds its library under tmp/a and executes cold (miss +
    promote into the shared task_cache). Run B builds a SEPARATE library
    under tmp/b with identical file bytes and executes against the same
    home. Because R1 makes leaf ids content-addressed, B's cache_keys
    match A's and B is a full cache hit — no executor fires.
    """
    task_a = _build_pipeline_task(tmp_path / "a")
    snap_a = capture_run(virtual_runtime, task_a)
    assert snap_a.executed_steps, "run A executed zero steps (bad fixture)"

    task_b = _build_pipeline_task(tmp_path / "b")

    # Precondition: content-addressed identity makes the two independently
    # built plans share a key. Pre-R1 this differed (random leaf ids).
    assert task_b.GetKey() == task_a.GetKey(), (
        "fresh library over identical bytes produced a different task key; "
        "leaf identity is not content-stable, so cross-run reuse is impossible"
    )

    clear_trace(virtual_runtime)
    snap_b = capture_run(virtual_runtime, task_b)
    assert snap_b.executed_steps == (), (
        f"run B re-executed steps {snap_b.executed_steps}; a fresh run on "
        f"identical inputs should be a full cache hit"
    )

    # Results-correctness invariant: a hit reproduces the miss's outputs.
    assert snap_b.result_fingerprints == snap_a.result_fingerprints, (
        "cross-run cache hit produced different output payloads than the "
        "original run — reentrancy must be results-preserving"
    )


def test_perturbed_inputs_get_distinct_identity(tmp_path, virtual_runtime):
    """R2 (negative): different input bytes must NOT collide.

    Content-addressing keys on file bytes, so a leaf whose payload differs
    must mint a different id. Guards against a path-only identity that
    would false-hit run A's cache when the file at that path changed.
    Built by adding the same path under two libraries with different bytes.
    """
    from metasmith.models.libraries import DataInstanceLibrary

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
        "perturbed input bytes minted the same leaf id — content-addressing "
        "is not sensitive to file contents"
    )


def test_leaf_id_portable_across_abs_and_rel_path(tmp_path, virtual_runtime):
    """F2: the leaf id folds the LIBRARY-RELATIVE path, not the raw argument.

    Two independent runs may add the same file via a relative path on one host
    and an absolute path (with a different home root) on another. Folding the
    raw `str(path)` made their leaf ids diverge → a spurious cross-run /
    cross-host cache miss. The id must be identical as long as the file's
    library-relative location and bytes match.
    """
    from metasmith.models.libraries import DataInstanceLibrary

    types_path = build_types_library(tmp_path, TYPE_NAMES)

    def _leaf_id(where: str, use_absolute_arg: bool) -> str:
        lib = DataInstanceLibrary(tmp_path / where)
        lib.Purge()
        lib.AddTypeLibrary(types_path, namespace="cf")
        (lib.location / "sub").mkdir(parents=True, exist_ok=True)
        (lib.location / "sub" / "data.txt").write_text("payload\n", encoding="utf-8")
        arg = (lib.location / "sub" / "data.txt") if use_absolute_arg else Path("sub/data.txt")
        lib.AddItem(arg, "cf::seed")
        return lib.Get(arg).instance_id

    id_rel = _leaf_id("relroot.xgdb", use_absolute_arg=False)
    id_abs = _leaf_id("a/deeper/absroot.xgdb", use_absolute_arg=True)
    assert id_rel == id_abs, (
        "same library-relative path + bytes minted different leaf ids for "
        "relative vs absolute AddItem arguments; cross-host reuse would miss"
    )


def test_leaf_random_optout_disables_cross_run(tmp_path, virtual_runtime):
    """R2/R3: METASMITH_LEAF_RANDOM=1 restores legacy random leaf ids.

    With the opt-out set, two independently built libraries over identical
    bytes mint DIFFERENT leaf ids, so their task keys diverge — the
    pre-R1 behaviour, available as a kill-switch.
    """
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
