"""Flow axis 6 — per-item provenance on a grouped slot <LP6, LP8>.

Catalog source: tests/flow/AGENTS.md.

Stimulus: `mock_transforms.labelled_collection()` — the ppanggolin shape reduced
to its bones. A `root` groups everything; each `label` is a user-supplied name
under that root; each `assembly` descends from a label and is transformed
per-sample into a `bam`. The collecting step at `group_by=root` receives N
labels and N bams at once and must say which goes with which.

**What this tier can and cannot prove.** This suite is plan-level by charter, and
its runtime carries a *produced* slot as the single archetype the plan holds
rather than materializing the fan-out (`sar` reports 1 for it). So the N-to-N
pairing of labels against bams is a real-channel case and lives in
`tests/e2e/docker/`; what is pinned here is the mechanism underneath it, on the
slot the runtime does materialize:

  - the collecting task is handed one index map **per item**, not one unioned
    map per slot — the whole of the Groovy change, and the thing whose absence
    made this impossible;
  - those maps stay aligned 1:1 with the files they describe;
  - each carries an id distinguishing its own item, so a resolver has something
    to join on;
  - and none of it escapes the task.

The resolver that reads all this is pinned in
`tests/unit/test_context_provenance.py`, including the mispairing and ambiguity
cases. The two together are the fast proof.
"""

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
    """(payload, slk) off the collecting step's single invocation."""
    metas = sorted(workspace.glob("nxf_work/step_*/*/.command.metadata"))
    assert metas, f"no step metadata under {workspace}"
    raw: dict[str, str] = {}
    for line in metas[-1].read_text().splitlines():
        k, _, v = line.partition(" ")
        raw[k] = v
    payload = LinPayload.from_json(raw["lin"])
    assert len(payload.entries) == 1, "collecting step should be one batch member"
    return payload, json.loads(raw["slk"])


# ---------------------------------------------------------------------------
# LP6 — the task is handed per-item ancestry, aligned with its per-item files
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("shuffle", [False, True], ids=["declared", "reversed"])
def test_each_item_of_a_grouped_slot_carries_its_own_index(
    virtual_runtime, tmp_path, shuffle
):
    """<LP6> Three labels arrive as three distinct index maps, not one union.

    Before per-item provenance, `group()` folded every item's index into a
    single map and deduped it, so this slot would present one map for three
    files and nothing could tell them apart. `shuffle` reverses the order the
    labels are registered in: the pairing is by ancestry, so it must not care.
    """
    plan = build_labelled_collection_plan(tmp_path, n_samples=3, shuffle=shuffle)
    workspace, _ = _stage_and_run(virtual_runtime, plan)
    payload, slk = _wire(workspace)

    files, prov = payload.file_groups(0), payload.provenance_groups(0)
    assert prov, (
        "the collecting task carries no PROV -- nothing downstream can pair "
        "these slots"
    )
    # The transform declares `root, label, item`; both structures are emitted
    # positionally against `model.requires`, so slot 1 is the labels.
    labels, label_prov = files[1], prov[1]
    assert {Path(f).name for f in labels} == {
        f"sample_{i:02d}.label" for i in range(3)
    }
    assert len(label_prov) == 3, (
        f"three label files but {len(label_prov)} provenance maps -- the "
        "per-item indexes were flattened or unioned on the way to the task"
    )

    # Each map must name its own item distinctly, or a resolver has no join key.
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
    """<LP6> `PROV[s][i]` describes `FILES[s][i]`, for every slot.

    They are built from one value in one closure precisely so this holds by
    construction; bootstrap re-checks it and drops provenance rather than serve
    a mispairing, so a divergence here would be silent.
    """
    plan = build_labelled_collection_plan(tmp_path, n_samples=3)
    workspace, _ = _stage_and_run(virtual_runtime, plan)
    payload, _ = _wire(workspace)
    files, prov = payload.file_groups(0), payload.provenance_groups(0)
    assert len(prov) == len(files), (
        f"PROV has {len(prov)} slots, FILES has {len(files)}"
    )
    for i, (f, p) in enumerate(zip(files, prov)):
        assert len(f) == len(p), f"slot {i}: {len(f)} files but {len(p)} maps"


# ---------------------------------------------------------------------------
# LP8 — it does not escape the task
# ---------------------------------------------------------------------------


def test_provenance_is_stripped_before_it_reaches_a_result(virtual_runtime, tmp_path):
    """<LP8> `PROV` is task-scoped.

    Left in, it rides into every descendant index through `_post`, grows without
    bound, and lands in promoted shard manifests — where the cache-hit path
    would render a nested map into a generated `.nf` as silent garbage.
    """
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
