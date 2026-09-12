"""Adding a sample must not re-run the samples already done.

Pins I7 of the annotation-trio investigation. The trio runs at two samples,
then at three in the same agent home with the first two samples untouched on
disk. The keys the run minted are read back from the trace: a per-sample step
keeps every key it already had and adds one, and a database step does not know
how many samples there are. The trio's merges are per sample too -- each folds
one sample's chunks -- so they answer to the first rule.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.telemetry import TraceIndex

from ..fixtures.trio import build_named_inputs, solve_trio
from ._cache_harness import capture_run, clear_trace


# Named rather than derived, because the plan gives no handle to derive them
# from: the solver folds the trio into one unique case, so every step after
# `prodigal` carries a single plan instance whatever the sample count.
PER_SAMPLE = [
    "prodigal",
    "chunkOrfsForAnnotation",
    "diamond_uniref50",
    "kofamscan",
    "interproscan",
]
# `group_by=parent_orfs`: one task per sample, folding that sample's chunks.
MERGES = [
    "merge_diamond_uniref50",
    "merge_kofamscan",
    "merge_interproscan",
]
DATABASES = [
    "downloadUniRef50DB",
    "downloadKofamDB",
    "downloadInterProScanDB",
]


def keys_by_step(workspace: Path) -> dict[str, set[str]]:
    trace = TraceIndex.read(workspace / "_metasmith" / "trace.jsonl")
    out: dict[str, set[str]] = {}
    for ev in trace.events:
        if ev.status not in ("hit", "promoted", "miss") or not ev.step_name:
            continue
        out.setdefault(ev.step_name, set()).add(ev.task_hash)
    return out


@pytest.fixture
def trio_at_two_and_three(virtual_runtime, tmp_path, metasmith_libraries_root):
    root = metasmith_libraries_root
    at = tmp_path / "inputs"
    two = capture_run(virtual_runtime, solve_trio(root, build_named_inputs(root, at, ["s00", "s01"])))
    clear_trace(virtual_runtime)
    three = capture_run(
        virtual_runtime, solve_trio(root, build_named_inputs(root, at, ["s00", "s01", "s02"]))
    )
    return keys_by_step(two.workspace), keys_by_step(three.workspace)


def test_a_per_sample_step_keeps_the_keys_it_already_minted(trio_at_two_and_three):
    at2, at3 = trio_at_two_and_three
    missing = [n for n in PER_SAMPLE if n not in at2 or n not in at3]
    assert not missing, f"the trio no longer has these steps: {missing}"
    rerun = sorted(n for n in PER_SAMPLE if not at2[n] <= at3[n])
    assert not rerun, (
        f"adding one sample re-keys [{len(rerun)}] of [{len(PER_SAMPLE)}] per-sample "
        f"step(s), so every sample already done is run again: {rerun}"
    )
    narrow = sorted(n for n in PER_SAMPLE if len(at3[n]) != 3)
    assert not narrow, f"a per-sample step at three samples does not hold three keys: {narrow}"


def test_a_per_sample_merge_keeps_its_keys_too(trio_at_two_and_three):
    at2, at3 = trio_at_two_and_three
    rerun = sorted(n for n in MERGES if not at2[n] <= at3[n])
    assert not rerun, f"adding one sample re-keys a per-sample merge: {rerun}"
    narrow = sorted(n for n in MERGES if len(at3[n]) != 3)
    assert not narrow, f"a merge at three samples does not hold three keys: {narrow}"


def test_a_database_step_is_untouched_by_the_sample_count(trio_at_two_and_three):
    at2, at3 = trio_at_two_and_three
    moved = sorted(n for n in DATABASES if at2[n] != at3[n])
    assert not moved, f"the sample count moved a database step's key: {moved}"
