from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.paths import DEFERRED, DeferredPathError, is_deferred
from metasmith.models.workflow import WorkflowTask
from metasmith.testing import mock_transforms as mt

from .conftest import _build_transform_lib, _build_type_lib, _generate_plan


def _deferred_task(tmp_path: Path, n_samples: int = 2) -> WorkflowTask:
    tmp_path.mkdir(parents=True, exist_ok=True)
    types_path = _build_type_lib(tmp_path / "types.yml")

    samples = DataInstanceLibrary(tmp_path / "samples.xgdb")
    samples.AddTypeLibrary(types_path, namespace="mock")
    for _ in range(n_samples):
        samples.AddItem(DEFERRED, "mock::assembly")
    samples.Save()

    transforms = {}
    transforms.update(mt.identity_transform("mock::assembly", "mock::bam"))
    tr_lib = _build_transform_lib(tmp_path / "tr", types_path, transforms)

    plan = _generate_plan(
        samples,
        tr_lib,
        sample_dtype="assembly",
        target_props=[{"bam"}],
        target_names=["bam"],
    )
    return WorkflowTask(
        ok=True, plan=plan, data_libraries=[samples], transform_libraries=[tr_lib]
    )


def test_a_workflow_of_deferred_inputs_solves(tmp_path: Path):
    task = _deferred_task(tmp_path)
    assert task.plan.steps, "planner produced no steps over deferred inputs"
    assert all(is_deferred(inst.path) for inst in task.plan.given)


def test_the_same_spec_solves_to_the_same_key(tmp_path: Path):
    first = _deferred_task(tmp_path / "a")
    paths = [inst.path for inst in first.plan.given]

    (tmp_path / "b").mkdir(parents=True, exist_ok=True)
    types_path = _build_type_lib(tmp_path / "b" / "types.yml")
    samples = DataInstanceLibrary(tmp_path / "b" / "samples.xgdb")
    samples.AddTypeLibrary(types_path, namespace="mock")
    for p in paths:
        samples.AddItem(p, "mock::assembly")
    samples.Save()
    tr_lib = _build_transform_lib(
        tmp_path / "b" / "tr",
        types_path,
        dict(mt.identity_transform("mock::assembly", "mock::bam")),
    )
    plan = _generate_plan(
        samples, tr_lib, sample_dtype="assembly",
        target_props=[{"bam"}], target_names=["bam"],
    )
    assert plan._key == first.plan._key


def test_the_dag_draws(tmp_path: Path):
    task = _deferred_task(tmp_path)
    svg = task.plan.BuildDAG().to_svg()
    assert svg.lstrip().startswith("<"), "deferred plan did not render"


def test_staging_refuses_and_names_the_rows(tmp_path: Path):
    task = _deferred_task(tmp_path, n_samples=2)
    with pytest.raises(DeferredPathError) as exc:
        task.RefuseIfDeferred()
    msg = str(exc.value)
    assert "2 input(s) have no path yet" in msg
    for inst in task.DeferredInputs():
        assert str(inst.path) in msg


def test_a_bound_workflow_is_not_refused(tmp_path: Path):
    from .conftest import build_linear_plan

    built = build_linear_plan(tmp_path, n_steps=1)
    built.as_task().RefuseIfDeferred()
