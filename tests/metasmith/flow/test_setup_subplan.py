from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.agents import Spec
from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.remote import Source
from metasmith.models.workflow import NextflowGenContext, WorkflowTask
from metasmith.models.workflow.setup import NoSetupStepsError, setup_subplan
from metasmith.testing import mock_transforms as mt
from metasmith.testing.pool_fixtures import pool_backed

from .conftest import _build_samples_lib, _build_transform_lib, _build_type_lib

_HEADER = """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
"""

FETCH = _HEADER + """
src = model.AddRequirement(lib.GetType("mock::container"))
db = model.AddProduct(lib.GetType("mock::h1"))

def protocol(context: ExecutionContext):
    Path("db.txt").write_text("db")
    return ExecutionResult(manifest=[{db: Path("db.txt")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=src)
"""

ANNOTATE = _HEADER + """
db = model.AddRequirement(lib.GetType("mock::h1"))
asm = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::annotated"))

def protocol(context: ExecutionContext):
    Path("out.txt").write_text("annotated")
    return ExecutionResult(manifest=[{out: Path("out.txt")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
"""


def _solve(tmp_path: Path, transforms: dict[str, str], target: str, resources=True) -> WorkflowTask:
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, n_samples=2, dtype="assembly")
    refs = DataInstanceLibrary(tmp_path / "refs.xgdb")
    refs.AddTypeLibrary(types_path, namespace="mock")
    (refs.location / "image.txt").write_text("image")
    refs.AddItem(Path("image.txt"), "mock::container")
    pool_backed(refs)
    refs.Save()
    tr_lib = _build_transform_lib(tmp_path / "tr", types_path, transforms)
    return Spec.SolveViews(
        samples=list(samples.AsSamples("mock::assembly")),
        resources=[refs] if resources else [],
        transforms=[tr_lib],
        targets=[target],
    )


def _slots(task: WorkflowTask, home: Path, monkeypatch) -> dict[str, list[str]]:
    monkeypatch.setattr(AgentPaths, "HOME_ROOT", home)
    monkeypatch.setattr(AgentPaths, "WORK_ROOT", home / "_ws")
    task_path = AgentPaths.to_task(task.GetKey())
    task_path.parent.mkdir(parents=True, exist_ok=True)
    task.SaveAs(Source.FromLocal(task_path))
    staged = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])
    ws = task_path.parent.parent
    staged.PrepareNextflow(NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW, work_dir=ws, external_work=ws,
        home_dir=home, external_home=home, runtime=Runtime.APPTAINER,
        resources_file=AgentPaths.NXF_RES, cache_root=home / "cache",
    ))
    return {
        step.transform.name: sorted(i.instance_id for g in step.produces for i in g)
        for step in staged.plan.steps
    }


def test_the_slice_keeps_only_the_steps_that_read_no_sample(tmp_path):
    task = _solve(tmp_path, {"fetch": FETCH, "annotate": ANNOTATE}, "mock::annotated")
    assert task.ok, task.plan.hints
    sub = setup_subplan(task)
    assert [s.transform.name for s in sub.plan.steps] == ["fetch"]
    assert [t.name for t in sub.plan.targets] == ["mock::h1"]
    assert sub.sample_library_count == 0
    assert all(lib.GetKey() != task.data_libraries[0].GetKey() for lib in sub.data_libraries)


def test_a_sliced_step_keeps_the_full_plans_cache_slot(tmp_path, monkeypatch):
    task = _solve(tmp_path, {"fetch": FETCH, "annotate": ANNOTATE}, "mock::annotated")
    sub = setup_subplan(task)
    full = _slots(task, tmp_path / "full_home", monkeypatch)
    sliced = _slots(sub, tmp_path / "sub_home", monkeypatch)
    assert sliced["fetch"] == full["fetch"]


def test_a_slice_after_a_sample_step_is_numbered_from_one(tmp_path, monkeypatch):
    # On fir a setup run of metagenomics_from_paired_reads kept steps 1-5, 7 and 8: step 6
    # reads a sample. The bootstrap read step 7 as the slice's seventh step, so
    # buildUniRef50DB ran buildPhyloflashDB, and step 8 ran off the end of the list.
    task = _solve(tmp_path, {"fetch": FETCH, "annotate": ANNOTATE}, "mock::annotated")
    steps = sorted(task.plan.steps, key=lambda s: s.transform.name != "annotate")
    for order, step in enumerate(steps, start=1):
        step.order = order
    task.plan.steps = steps
    sub = setup_subplan(task)
    task.SaveAs(Source.FromLocal(tmp_path / "full"))
    sub.SaveAs(Source.FromLocal(tmp_path / "sub"))
    loaded = WorkflowTask.Load(tmp_path / "sub")
    assert [(s.order, s.transform.name) for s in loaded.plan.steps] == [(1, "fetch")]
    assert [t.producing_step.order for t in loaded.plan.targets] == [1]
    assert [(s.order, s.transform.name) for s in task.plan.steps] == [(1, "annotate"), (2, "fetch")]
    full = _slots(task, tmp_path / "full_home", monkeypatch)
    sliced = _slots(sub, tmp_path / "sub_home", monkeypatch)
    assert sliced["fetch"] == full["fetch"]


def test_the_slice_survives_a_save_and_load(tmp_path):
    task = _solve(tmp_path, {"fetch": FETCH, "annotate": ANNOTATE}, "mock::annotated")
    task.SaveAs(Source.FromLocal(tmp_path / "bundle"))
    loaded = WorkflowTask.Load(tmp_path / "bundle")
    assert loaded.sample_library_count == 1
    assert [s.transform.name for s in setup_subplan(loaded).plan.steps] == ["fetch"]


def test_a_plan_where_every_step_reads_a_sample_has_no_slice(tmp_path):
    task = _solve(tmp_path, mt.identity_transform("mock::assembly", "mock::bam"), "mock::bam")
    with pytest.raises(NoSetupStepsError):
        setup_subplan(task)


def test_a_bundle_from_before_the_sample_record_is_refused(tmp_path):
    task = _solve(tmp_path, {"fetch": FETCH, "annotate": ANNOTATE}, "mock::annotated")
    task.sample_library_count = None
    with pytest.raises(AssertionError, match="generate it again"):
        setup_subplan(task)
