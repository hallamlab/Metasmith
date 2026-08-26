"""Adding a sample must not re-run the samples already done.

Pins I7 of the annotation-trio investigation. A step's cache entry is keyed on
the whole set of inputs the step consumes, not on the inputs one invocation
consumes, so a twelve-sample workflow that gains a thirteenth sample re-runs all
thirteen. The steps that genuinely consume every sample -- the merges -- are the
only ones that should re-key.

Reads the decisions directly rather than running anything: `compute_cache_decisions`
is where a key is minted, and running nextflow to watch it would say the same
thing an hour later.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.workflow import NextflowGenContext
from metasmith.models.workflow.cache_decisions import compute_cache_decisions

from ..fixtures.trio import build_inputs, solve_trio


def _decisions(lib_root: Path, at: Path, n: int) -> dict[str, dict]:
    at.mkdir(parents=True, exist_ok=True)
    task = solve_trio(lib_root, build_inputs(lib_root, at, n))
    context = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=at / "ws",
        external_work=at / "ws",
        home_dir=at / "home",
        external_home=at / "home",
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
        cache_root=at / "home" / "task_cache",
    )
    raw = compute_cache_decisions(task, context)
    assert raw, "no cache decisions were computed"
    by_name = {}
    for order, decision in raw.items():
        by_name[task.plan.steps[order - 1].transform.name] = decision
    return by_name


def _keys(decision: dict) -> set[str]:
    # A per-invocation key belongs on the batch; until it does, every batch of a
    # step answers to the step's one key. Written so both shapes read the same.
    step_key = decision["cache_key"].hex()
    return {
        (b["cache_key"].hex() if isinstance(b.get("cache_key"), bytes) else step_key)
        for b in decision["batches"]
    } or {step_key}


@pytest.fixture(scope="module")
def trio_at_two_and_three(metasmith_libraries_root, tmp_path_factory):
    root = tmp_path_factory.mktemp("sample_addition")
    return (
        _decisions(metasmith_libraries_root, root / "n2", 2),
        _decisions(metasmith_libraries_root, root / "n3", 3),
    )


# Named rather than derived, because the plan gives no handle to derive them
# from: the solver folds the trio into one unique case, so every step after
# `prodigal` carries a single plan instance whatever the sample count, and a
# merge is indistinguishable from a per-sample annotator by shape alone.
PER_SAMPLE = [
    "prodigal",
    "chunkOrfsForAnnotation",
    "diamond_uniref50",
    "kofamscan",
    "interproscan",
]
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


def test_a_per_sample_step_keeps_the_keys_it_already_minted(trio_at_two_and_three):
    at2, at3 = trio_at_two_and_three
    missing = [n for n in PER_SAMPLE if n not in at2 or n not in at3]
    assert not missing, f"the trio no longer has these steps: {missing}"
    rerun = sorted(n for n in PER_SAMPLE if not _keys(at2[n]) <= _keys(at3[n]))
    assert not rerun, (
        f"adding one sample re-keys [{len(rerun)}] of [{len(PER_SAMPLE)}] per-sample "
        f"step(s), so every sample already done is run again: {rerun}"
    )


def test_a_step_that_consumes_every_sample_still_rekeys(trio_at_two_and_three):
    # The other half of the contract, and green today: a merge really does see a
    # different input set when a sample is added, so its key must move.
    at2, at3 = trio_at_two_and_three
    unchanged = sorted(n for n in MERGES if _keys(at2[n]) == _keys(at3[n]))
    assert not unchanged, (
        f"a merge over every sample kept its key when a sample was added: {unchanged}"
    )


def test_a_database_step_is_untouched_by_the_sample_count(trio_at_two_and_three):
    # Green today, and it must stay green: a database shard costs gigabytes to
    # rebuild and nothing about it depends on how many samples are in the run.
    at2, at3 = trio_at_two_and_three
    moved = sorted(n for n in DATABASES if _keys(at2[n]) != _keys(at3[n]))
    assert not moved, f"the sample count moved a database step's key: {moved}"
