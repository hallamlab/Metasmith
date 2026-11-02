from pathlib import Path

import pytest

from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.solver import Transform
from metasmith.models.workflow import (
    WorkflowPlan,
    WorkflowTask,
    NextflowGenContext,
)
from metasmith.testing.mock_transforms import (
    alignment_transform,
    binner_transforms,
)

from .conftest import create_transform_library


def _binning_task(mock_samples, mock_types, temp_dir, publish_intermediates: bool):
    transforms = alignment_transform() | binner_transforms()
    tr_lib = create_transform_library(temp_dir / "tr_publish", mock_types, transforms)

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
    plan.publish_intermediates = publish_intermediates

    return WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=[mock_samples],
        transform_libraries=[tr_lib],
    )


def _stage(task: WorkflowTask, work_dir: Path) -> str:
    work_dir.mkdir(parents=True, exist_ok=True)
    context = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=work_dir,
        external_work=work_dir,
        home_dir=Path("/msm_home"),
        external_home=work_dir.parent,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    )
    task.PrepareNextflow(context)
    return (work_dir / AgentPaths.NXF_WORKFLOW).read_text()


class TestPublishIntermediates:
    def test_enabled_publishes_all(self, mock_samples, mock_types, temp_dir):
        task = _binning_task(mock_samples, mock_types, temp_dir, publish_intermediates=True)
        bam_order = None
        for step in task.plan.steps:
            for group in step.produces:
                for inst in group:
                    if inst.dtype_name == "mock::bam":
                        bam_order = step.order
                        break
        assert bam_order is not None, "fixture should produce mock::bam as intermediate"

        nxf = _stage(task, temp_dir / "ws_default")

        for tname in ("metabat2_bins", "maxbin2_bins", "concoct_bins"):
            assert f"path '{tname}'" in nxf, f"target {tname} should be published"

        assert f"path '{bam_order}_mock-bam'" in nxf

    def test_disabled_only_targets(self, mock_samples, mock_types, temp_dir):
        task = _binning_task(mock_samples, mock_types, temp_dir, publish_intermediates=False)
        nxf = _stage(task, temp_dir / "ws_targets_only")

        for tname in ("metabat2_bins", "maxbin2_bins", "concoct_bins"):
            assert f"path '{tname}'" in nxf

        assert "_mock-bam" not in nxf
        assert "path 'mock-bam'" not in nxf

    def test_pack_unpack_roundtrip_preserves_flag(self, mock_samples, mock_types, temp_dir):
        task = _binning_task(mock_samples, mock_types, temp_dir, publish_intermediates=False)
        packed = task.plan.Pack()
        assert packed["publish_intermediates"] is False

        libraries = {task.transform_libraries[0].GetKey(): task.transform_libraries[0]}
        libraries[mock_samples.GetKey()] = mock_samples
        restored = WorkflowPlan.Unpack(packed, libraries)
        assert restored.publish_intermediates is False

    def test_default_is_targets_only(self, mock_samples, mock_types, temp_dir):
        task = _binning_task(mock_samples, mock_types, temp_dir, publish_intermediates=True)
        assert WorkflowPlan(given=[], targets=[], steps=[]).publish_intermediates is False

        # An index with no flag reads the way the dataclass defaults, so the two
        # cannot drift into publishing different things for the same plan.
        packed = task.plan.Pack()
        packed.pop("publish_intermediates", None)
        libraries = {task.transform_libraries[0].GetKey(): task.transform_libraries[0]}
        libraries[mock_samples.GetKey()] = mock_samples
        restored = WorkflowPlan.Unpack(packed, libraries)
        assert restored.publish_intermediates is False
