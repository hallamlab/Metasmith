"""The per-task rootfs override, from staging through to the tool environment.

`Agent.StageWorkflow(task, rootfs=...)` forces how this task's step images are
materialised. It has no run-time transport of its own: it rides in the staged
step meta, the same channel the static GPU declaration uses, which is what
"from staging onwards" means. Precedence then falls out of absence -- meta line
present is the task's answer, meta line absent leaves the agent's own tendency
in charge -- and that is also what keeps an un-overridden workspace
byte-identical to one compiled before the knob existed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.env import Environment, ContainerDef, Rootfs, Runtime
from metasmith.models.libraries.execution import _materialised_test
from metasmith.models.solver import Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowTask
from metasmith.testing.mock_transforms import alignment_transform, binner_transforms

from .conftest import create_transform_library, stage_task


def _stage(tmp_path, mock_samples, mock_types, rootfs=None):
    transforms = alignment_transform() | binner_transforms()
    tr_lib = create_transform_library(tmp_path / "tr", mock_types, transforms)
    given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
    target_model = Transform()
    target_model.AddRequirement(properties={"bins", "method:metabat2"})
    plan = WorkflowPlan.Generate(
        given=given, transforms=[tr_lib],
        target_names=["metabat2_bins"], target_model=target_model,
    )
    assert isinstance(plan, WorkflowPlan), plan
    task = WorkflowTask(ok=True, plan=plan, data_libraries=[mock_samples], transform_libraries=[tr_lib])
    return stage_task(task, rootfs=rootfs)


def _metas(workspace: Path, staged: WorkflowTask) -> list[str]:
    return [(workspace / f"workflow.step_{s.order}.meta").read_text() for s in staged.plan.steps]


def test_no_override_writes_no_line(virtual_runtime, tmp_path, mock_samples, mock_types):
    _, workspace, staged = _stage(tmp_path, mock_samples, mock_types)
    metas = _metas(workspace, staged)
    assert metas
    for m in metas:
        assert "rootfs" not in m


def test_override_reaches_every_step(virtual_runtime, tmp_path, mock_samples, mock_types):
    _, workspace, staged = _stage(tmp_path, mock_samples, mock_types, rootfs=Rootfs.SANDBOX)
    metas = _metas(workspace, staged)
    assert metas
    for m in metas:
        assert "rootfs sandbox" in m.splitlines()


@pytest.mark.parametrize("mode,expected", [
    (Rootfs.SIF, ".sif"),
    (Rootfs.SANDBOX, ".sandbox"),
])
def test_a_forced_mode_ignores_the_other_artifact_on_disk(mode, expected):
    """The half that makes the override more than advisory.

    `_ExecInEnv` skips materialising when the image is already there. Accepting
    either artifact is how the old host-level override managed to be inert: a
    `.sandbox` left in a shared store satisfied the test, materialising was
    skipped, and the run command's ternary then preferred the directory.
    """
    env = Environment(
        image="docker://quay.io/example/tool:1.0", runtime=Runtime.APPTAINER,
        rootfs=mode, container=ContainerDef(cache=Path("/cache")),
    )
    test = _materialised_test(env)
    other = ".sandbox" if expected == ".sif" else ".sif"
    assert expected in test and other not in test
    assert "||" not in test


def test_auto_accepts_whichever_artifact_materialising_produced():
    env = Environment(
        image="docker://quay.io/example/tool:1.0", runtime=Runtime.APPTAINER,
        container=ContainerDef(cache=Path("/cache")),
    )
    test = _materialised_test(env)
    assert ".sif" in test and ".sandbox" in test and "||" in test
