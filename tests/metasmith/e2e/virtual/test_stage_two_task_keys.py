from __future__ import annotations

from metasmith.models.solver import Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowTask
from metasmith.testing.mock_transforms import alignment_transform, binner_transforms

from .conftest import create_transform_library, stage_task


def _task(tmp_path, mock_samples, mock_types, methods: list[str]) -> WorkflowTask:
    tr_lib = create_transform_library(tmp_path / "tr", mock_types, alignment_transform() | binner_transforms())
    target_model = Transform()
    for m in methods:
        target_model.AddRequirement(properties={"bins", f"method:{m}"})
    plan = WorkflowPlan.Generate(
        given=[[sv] for sv in mock_samples.AsSamples("mock::assembly")],
        transforms=[tr_lib],
        target_names=[f"{m}_bins" for m in methods],
        target_model=target_model,
    )
    return WorkflowTask(ok=True, plan=plan, data_libraries=[mock_samples], transform_libraries=[tr_lib])


def test_one_library_staged_under_two_task_keys_keeps_its_given_ids(virtual_runtime, tmp_path, mock_samples, mock_types):
    first = _task(tmp_path, mock_samples, mock_types, ["metabat2"])
    second = _task(tmp_path, mock_samples, mock_types, ["metabat2", "concoct"])
    assert first.GetKey() != second.GetKey()

    ids = []
    for task in (first, second):
        _, _, staged = stage_task(task)
        ids.append({(g.dtype_name, g.instance_id) for g in staged.plan.given})
    assert ids[0] and ids[0] <= ids[1], ids
    assert {i for _, i in ids[0]} == {g.instance_id for g in first.plan.given}
