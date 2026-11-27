from metasmith.models.workflow import Case
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

from tests.metasmith.support import create_transform_library


def _binning_task(mock_samples, mock_types, tmp_path, keep_intermediates: bool):
    transforms = alignment_transform() | binner_transforms()
    tr_lib = create_transform_library(tmp_path / "tr_publish", mock_types, transforms)

    given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
    target_model = Transform()
    target_model.AddRequirement(properties={"bins", "method:metabat2"})
    target_model.AddRequirement(properties={"bins", "method:maxbin2"})
    target_model.AddRequirement(properties={"bins", "method:concoct"})
    target_names = ["metabat2_bins", "maxbin2_bins", "concoct_bins"]

    plan = WorkflowPlan.Generate(
        cases=Case.ByShape(given, target=target_model, target_names=target_names),
        transforms=[tr_lib],
    )
    assert isinstance(plan, WorkflowPlan)
    plan.keep_intermediates = keep_intermediates

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


class TestKeepIntermediates:
    def test_enabled_publishes_all_and_deletes_nothing(self, mock_samples, mock_types, tmp_path):
        task = _binning_task(mock_samples, mock_types, tmp_path, keep_intermediates=True)
        bam_order = next(
            step.order for step in task.plan.steps for group in step.produces
            for inst in group if inst.dtype_name == "mock::bam"
        )

        nxf = _stage(task, tmp_path / "ws_default")

        for tname in ("metabat2_bins", "maxbin2_bins", "concoct_bins"):
            assert f"path '{tname}'" in nxf, f"target {tname} should be published"
        assert f"path '{bam_order}_mock-bam'" in nxf
        assert "o.footprint([probe: params.msm_probe, delete: false," in nxf

    def test_disabled_publishes_and_keeps_only_targets(self, mock_samples, mock_types, tmp_path):
        task = _binning_task(mock_samples, mock_types, tmp_path, keep_intermediates=False)
        nxf = _stage(task, tmp_path / "ws_targets_only")

        for tname in ("metabat2_bins", "maxbin2_bins", "concoct_bins"):
            assert f"path '{tname}'" in nxf
        assert "_mock-bam" not in nxf
        assert "path 'mock-bam'" not in nxf
        assert "o.footprint([probe: params.msm_probe, delete: true," in nxf

    def test_pack_unpack_roundtrip_preserves_flag(self, mock_samples, mock_types, tmp_path):
        task = _binning_task(mock_samples, mock_types, tmp_path, keep_intermediates=True)
        packed = task.plan.Pack()
        assert packed["keep_intermediates"] is True

        libraries = {task.transform_libraries[0].GetKey(): task.transform_libraries[0]}
        libraries[mock_samples.GetKey()] = mock_samples
        assert WorkflowPlan.Unpack(packed, libraries).keep_intermediates is True

        # A plan saved before the rename carries the old key.
        packed["publish_intermediates"] = packed.pop("keep_intermediates")
        assert WorkflowPlan.Unpack(packed, libraries).keep_intermediates is True

    def test_default_is_targets_only(self, mock_samples, mock_types, tmp_path):
        task = _binning_task(mock_samples, mock_types, tmp_path, keep_intermediates=True)
        assert WorkflowPlan(given=[], targets=[], steps=[]).keep_intermediates is False

        # An index with no flag reads the way the dataclass defaults, so the two
        # cannot drift into keeping different things for the same plan.
        packed = task.plan.Pack()
        packed.pop("keep_intermediates", None)
        libraries = {task.transform_libraries[0].GetKey(): task.transform_libraries[0]}
        libraries[mock_samples.GetKey()] = mock_samples
        restored = WorkflowPlan.Unpack(packed, libraries)
        assert restored.keep_intermediates is False
