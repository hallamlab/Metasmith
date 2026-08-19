from __future__ import annotations

import json
from pathlib import Path

import pytest

from metasmith.models.lineage import LinPayload

from .conftest import (
    _stage_and_run,
    build_labelled_collection_plan,
    run_and_load,
)


def _wire(workspace: Path):
    metas = sorted(workspace.glob("nxf_work/step_*/*/.command.metadata"))
    assert metas, f"no step metadata under {workspace}"
    raw: dict[str, str] = {}
    for line in metas[-1].read_text().splitlines():
        k, _, v = line.partition(" ")
        raw[k] = v
    payload = LinPayload.from_json(raw["lin"])
    assert len(payload.entries) == 1, "collecting step should be one batch member"
    return payload, json.loads(raw["slk"])


@pytest.mark.parametrize("shuffle", [False, True], ids=["declared", "reversed"])
def test_each_item_of_a_grouped_slot_carries_its_own_index(
    virtual_runtime, tmp_path, shuffle
):
    plan = build_labelled_collection_plan(tmp_path, n_samples=3, shuffle=shuffle)
    workspace, _ = _stage_and_run(virtual_runtime, plan)
    payload, slk = _wire(workspace)

    files, prov = payload.file_groups(0), payload.provenance_groups(0)
    assert prov, (
        "the collecting task carries no PROV -- nothing downstream can pair "
        "these slots"
    )
    labels, label_prov = files[1], prov[1]
    assert {Path(f).name for f in labels} == {
        f"sample_{i:02d}.label" for i in range(3)
    }
    assert len(label_prov) == 3, (
        f"three label files but {len(label_prov)} provenance maps -- the "
        "per-item indexes were flattened or unioned on the way to the task"
    )

    chans = set(slk.values())
    distinguishing = [
        c for c in chans
        if len({tuple(sorted(map(str, m.get(c, [])))) for m in label_prov}) == 3
    ]
    assert distinguishing, (
        f"no channel in {chans} gives the three labels distinct ids; their "
        f"index maps are {label_prov}"
    )


def test_provenance_stays_aligned_with_the_files_it_describes(
    virtual_runtime, tmp_path
):
    plan = build_labelled_collection_plan(tmp_path, n_samples=3)
    workspace, _ = _stage_and_run(virtual_runtime, plan)
    payload, _ = _wire(workspace)
    files, prov = payload.file_groups(0), payload.provenance_groups(0)
    assert len(prov) == len(files), (
        f"PROV has {len(prov)} slots, FILES has {len(files)}"
    )
    for i, (f, p) in enumerate(zip(files, prov)):
        assert len(f) == len(p), f"slot {i}: {len(f)} files but {len(p)} maps"


def test_provenance_is_stripped_before_it_reaches_a_result(virtual_runtime, tmp_path):
    plan = build_labelled_collection_plan(tmp_path, n_samples=2)
    _, lib = run_and_load(virtual_runtime, plan)
    seen = 0
    for ev in lib.find_invocations():
        seen += 1
        for key in (LinPayload.PROV_KEY, LinPayload.FILES_KEY):
            assert key not in (ev.consumes or {}), (
                f"[{key}] leaked into a recorded invocation's consumes"
            )
    assert seen > 0, "no invocations recorded; the run did not execute"
