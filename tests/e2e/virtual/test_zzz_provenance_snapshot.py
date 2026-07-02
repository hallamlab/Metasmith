"""TEMPORARY parity-snapshot harness for the single-point-of-provenance refactor.

Runs the 3-sample -> 3-binner fan-out pipeline and dumps a deterministic
telemetry snapshot (summary + per-output ancestor id sets) to $MSM_SNAPSHOT_OUT.
Delete this file at T5. Not part of the real suite (skipped unless the env var
is set).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from metasmith.agents import RunWorkflow
from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.solver import Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowTask
from metasmith.testing.mock_transforms import alignment_transform, binner_transforms

from .conftest import create_transform_library, stage_task


def _build_binning_task(tmp_path, mock_samples, mock_types) -> WorkflowTask:
    transforms = alignment_transform() | binner_transforms()
    tr_lib = create_transform_library(tmp_path / "tr", mock_types, transforms)
    given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
    target_model = Transform()
    target_model.AddRequirement(properties={"bins", "method:metabat2"})
    target_model.AddRequirement(properties={"bins", "method:maxbin2"})
    target_model.AddRequirement(properties={"bins", "method:concoct"})
    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[tr_lib],
        target_names=["metabat2_bins", "maxbin2_bins", "concoct_bins"],
        target_model=target_model,
    )
    assert isinstance(plan, WorkflowPlan)
    return WorkflowTask(
        ok=True, plan=plan, data_libraries=[mock_samples],
        transform_libraries=[tr_lib],
    )


@pytest.mark.skipif(not os.environ.get("MSM_SNAPSHOT_OUT"), reason="snapshot harness")
def test_provenance_snapshot(virtual_runtime, tmp_path, mock_samples, mock_types):
    task = _build_binning_task(tmp_path, mock_samples, mock_types)
    key, workspace, staged = stage_task(task)
    RunWorkflow(key=key, log_dir=Path("_metasmith/logs.virtual"),
                host=virtual_runtime.host, stub_delay=0.0)

    out = DataInstanceLibrary.Load(workspace / "results", attach_trace=True)

    # Per-output: dtype + producing transform + sorted ancestor id set.
    per_output = []
    for iid, dtype in sorted(out.manifest.items(), key=lambda kv: (kv[1], kv[0])):
        try:
            tr = out.get_transform_of(iid)
            tk = getattr(tr, "transform_key", getattr(tr, "source", "?"))
            anc = sorted({n.instance_id for n in out.walk_ancestors(iid)})
        except Exception as e:  # noqa: BLE001
            tk, anc = f"ERR:{type(e).__name__}", []
        per_output.append({"iid": iid, "dtype": dtype, "transform": tk,
                           "ancestors": anc, "n_ancestors": len(anc)})

    snapshot = {
        "summary": out.summary(),
        "n_outputs": len(per_output),
        # ancestor COUNTS are the behaviour-sensitive number (cartesian bug
        # inflates them); ids let us see structure.
        "ancestor_counts": sorted(o["n_ancestors"] for o in per_output),
        "by_dtype_ancestor_counts": sorted(
            (o["dtype"], o["n_ancestors"]) for o in per_output
        ),
        "outputs": per_output,
    }
    dest = Path(os.environ["MSM_SNAPSHOT_OUT"])
    dest.write_text(json.dumps(snapshot, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(f"\n[snapshot] wrote {dest} : {len(per_output)} outputs, "
          f"summary={snapshot['summary']['counts']}")
