from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.agents import RunWorkflow
from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowTask
from metasmith.testing.mock_transforms import pull_container_transform
from metasmith.testing.plan_oracle import PlanExecutionOracle

from .conftest import create_transform_library, stage_task


_CONTAINER_SUBTYPES = {
    "c_bbtools": {"container", "provides:bbtools"},
    "c_megahit": {"container", "provides:megahit"},
    "c_seqkit": {"container", "provides:seqkit"},
}


def _container_types(tmp_path) -> Path:
    types = DataTypeLibrary()
    types["container"] = Endpoint(properties={"container"})
    for name, props in _CONTAINER_SUBTYPES.items():
        types[name] = Endpoint(properties=set(props))
    types["pulled"] = Endpoint(properties={"pulled_container"})
    path = tmp_path / "container_types.yml"
    types.Save(path)
    return path


def _container_inputs(tmp_path, types_path) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(tmp_path / "containers.xgdb")
    lib.AddTypeLibrary(types_path, namespace="mock")
    for name in _CONTAINER_SUBTYPES:
        tool = name.split("_", 1)[1]
        f = lib.location / f"{tool}.oci"
        f.write_text(f"docker://example/{tool}:latest\n")
        lib.AddItem(Path(f"{tool}.oci"), f"mock::{name}")
    lib.Save()
    # Read it back: a plan refuses a given whose identity this process minted.
    return DataInstanceLibrary.Load(lib.location)


def _build_pull_task(tmp_path) -> WorkflowTask:
    types_path = _container_types(tmp_path)
    inputs = _container_inputs(tmp_path, types_path)
    tr_lib = create_transform_library(tmp_path / "tr", types_path, pull_container_transform())

    given = [[sv] for sv in inputs.AsSamples("mock::container")]
    assert len(given) == 3

    target_model = Transform()
    target_model.AddRequirement(properties={"pulled_container"})
    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[tr_lib],
        target_names=["pulled"],
        target_model=target_model,
    )
    assert isinstance(plan, WorkflowPlan)
    assert len(plan.steps) == 1
    return WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=[inputs],
        transform_libraries=[tr_lib],
    )


def test_virtual_multicontainer_pulls_all(virtual_runtime, tmp_path):
    task = _build_pull_task(tmp_path)
    key, workspace, staged = stage_task(task)
    RunWorkflow(key=key, log_dir=Path("_metasmith/logs.virtual"), host=virtual_runtime.host, stub_delay=0.0)

    output = DataInstanceLibrary.Load(workspace / "results")
    pulled = [v for v in output.manifest.values() if v == "mock::pulled"]
    assert len(pulled) == 3, (
        f"expected 3 pulled containers, got {len(pulled)}: {list(output.manifest.values())}"
    )

    oracle = PlanExecutionOracle(staged)
    oracle.validate_trace(virtual_runtime.parse_trace())
