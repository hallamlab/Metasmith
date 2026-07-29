from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.agents import RunWorkflow
from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowTask
from metasmith.testing.mock_transforms import alignment_transform, binner_transforms
from metasmith.testing.plan_oracle import PlanExecutionOracle

from .conftest import create_transform_library, stage_task



def _build_binning_task(tmp_path, mock_samples, mock_types) -> WorkflowTask:
    transforms = alignment_transform() | binner_transforms()
    tr_lib = create_transform_library(tmp_path / "tr", mock_types, transforms)

    given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
    target_model = Transform()
    target_model.AddRequirement(properties={"bins", "method:metabat2"})
    target_model.AddRequirement(properties={"bins", "method:maxbin2"})
    target_model.AddRequirement(properties={"bins", "method:concoct"})
    target_names = ["metabat2_bins", "maxbin2_bins", "concoct_bins"]

    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[tr_lib],
        target_names=target_names,
        target_model=target_model,
    )
    assert isinstance(plan, WorkflowPlan)
    return WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=[mock_samples],
        transform_libraries=[tr_lib],
    )



def _run(task: WorkflowTask, host: str) -> tuple[Path, WorkflowTask]:
    key, workspace, staged = stage_task(task)
    RunWorkflow(key=key, log_dir=Path("_metasmith/logs.virtual"), host=host, stub_delay=0.0)
    return workspace, staged



def test_virtual_full_pipeline_and_oracle(virtual_runtime, tmp_path, mock_samples, mock_types):
    task = _build_binning_task(tmp_path, mock_samples, mock_types)
    workspace, staged = _run(task, virtual_runtime.host)

    output = DataInstanceLibrary.Load(workspace / "results")
    names = set(output.manifest.values())
    assert "mock::metabat2_bins" in names
    assert "mock::maxbin2_bins" in names
    assert "mock::concoct_bins" in names

    events = virtual_runtime.parse_trace()
    assert any(e.get("type") == "relay_start" for e in events)
    oracle = PlanExecutionOracle(staged)
    oracle.validate_trace(events)



def test_virtual_bounce_path_is_exercised(virtual_runtime_bounce, tmp_path, mock_samples, mock_types):
    task = _build_binning_task(tmp_path, mock_samples, mock_types)
    _, staged = _run(task, virtual_runtime_bounce.host)

    events = virtual_runtime_bounce.parse_trace()
    assert any(e.get("type") == "relay_bounce" for e in events), "expected at least one relay bounce event"

    oracle = PlanExecutionOracle(staged)
    oracle.validate_trace(events)



def test_oracle_detects_missing_bootstrap_event(virtual_runtime, tmp_path, mock_samples, mock_types):
    task = _build_binning_task(tmp_path, mock_samples, mock_types)
    _, staged = _run(task, virtual_runtime.host)

    events = virtual_runtime.parse_trace()
    trimmed = []
    removed = False
    for event in reversed(events):
        if not removed and event.get("type") == "bootstrap_call":
            removed = True
            continue
        trimmed.append(event)
    trimmed = list(reversed(trimmed))

    oracle = PlanExecutionOracle(staged)
    with pytest.raises(AssertionError):
        oracle.validate_trace(trimmed)
