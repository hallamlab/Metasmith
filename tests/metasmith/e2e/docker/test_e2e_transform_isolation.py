import json
import pytest
from pathlib import Path

from metasmith.models.libraries import (
    DataInstanceLibrary,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowTask, METADATA_FILE
from metasmith.testing.transform_harness import TransformHarness, MockShell
from metasmith.testing.mock_transforms import (
    alignment_transform,
    batched_transform,
    branching_transforms,
    failing_transform,
)

from .conftest import create_transform_library


def _make_task(mock_samples, mock_types, temp_dir, transforms, target_props, target_name):
    tr_lib = create_transform_library(temp_dir, mock_types, transforms)
    given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
    target_model = Transform()
    target_model.AddRequirement(properties=target_props)
    target_names = [target_name]

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


class TestHarnessBasic:
    def test_simple_transform_produces_output(self, mock_samples, mock_types, temp_dir):
        task = _make_task(
            mock_samples, mock_types, temp_dir / "basic",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=temp_dir / "work_basic",
        )
        result = harness.run()
        assert result.success
        assert len(result.manifest) > 0

        has_output = any(
            (harness.work_dir / p).exists() if not p.is_absolute() else p.exists()
            for m in result.manifest
            for p in m.values()
        )
        assert has_output, "Transform should produce at least one output file"

    def test_metadata_format(self, mock_samples, mock_types, temp_dir):
        task = _make_task(
            mock_samples, mock_types, temp_dir / "meta",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=temp_dir / "work_meta",
        )
        meta_path = harness.write_metadata()
        assert meta_path.exists()

        content = meta_path.read_text()
        lines = content.strip().split("\n")
        assert len(lines) >= 8

        keys = [l.split(" ", maxsplit=1)[0] for l in lines]
        for expected in ["res", "lin", "fmt", "din", "dot", "inp", "out"]:
            assert expected in keys

        lin_line = [l for l in lines if l.startswith("lin ")][0]
        lin_data = json.loads(lin_line[4:])
        assert isinstance(lin_data, list)

    def test_input_files_accessible(self, mock_samples, mock_types, temp_dir):
        task = _make_task(
            mock_samples, mock_types, temp_dir / "inp",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=temp_dir / "work_inp",
        )
        input_map = harness.setup_inputs()
        assert len(input_map) > 0

        for dep, paths in input_map.items():
            for p in paths:
                assert p.exists() or p.is_symlink(), f"Input {p} should be accessible"

    def test_output_path_convention(self, mock_samples, mock_types, temp_dir):
        task = _make_task(
            mock_samples, mock_types, temp_dir / "outpath",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=temp_dir / "work_outpath",
        )
        harness.write_metadata()
        context = harness.build_context()

        step = task.plan.steps[0]
        out_dep = step.transform.model.produces[0][0]
        out_path = context.Output(out_dep)

        name = out_path.local.name
        parts = name.split("-", 2)
        assert len(parts) >= 3, f"Output name '{name}' should have batch-item-branch prefix"
        assert parts[0].isdigit(), f"First part '{parts[0]}' should be batch number"
        assert parts[1].isdigit(), f"Second part '{parts[1]}' should be item number"

    def test_mock_shell_interface(self):
        shell = MockShell()
        result = shell.Exec("test command", history=True)
        assert result.out == []
        assert result.err == []

        with shell:
            pass

        called = []
        shell.RegisterOnOut(lambda x: called.append(x))
        shell.RemoveOnOut(called.append)


class TestHarnessBatching:
    def test_batched_context_iterates(self, mock_samples, mock_types, temp_dir):
        task = _make_task(
            mock_samples, mock_types, temp_dir / "batch",
            batched_transform(batch_size=3), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=temp_dir / "work_batch",
        )
        harness.write_metadata()
        context = harness.build_context()

        batch_count = 0
        for _ in context.AsBatch():
            batch_count += 1
        assert batch_count >= 1

    def test_batch_outputs_per_item(self, mock_samples, mock_types, temp_dir):
        task = _make_task(
            mock_samples, mock_types, temp_dir / "batchout",
            batched_transform(batch_size=3), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=temp_dir / "work_batchout",
        )
        result = harness.run()
        assert result.success or len(result.manifest) > 0


class TestHarnessLineage:
    def test_lineage_metadata_correct(self, mock_samples, mock_types, temp_dir):
        task = _make_task(
            mock_samples, mock_types, temp_dir / "lin",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=temp_dir / "work_lin",
        )
        meta_path = harness.write_metadata()
        content = meta_path.read_text()
        lin_line = [l for l in content.split("\n") if l.startswith("lin")][0]
        lin_data = json.loads(lin_line[4:])

        assert isinstance(lin_data, list)
        assert len(lin_data) > 0

        for entry in lin_data:
            assert isinstance(entry, dict)
            assert "FILES" in entry

    def test_files_entry_correct(self, mock_samples, mock_types, temp_dir):
        task = _make_task(
            mock_samples, mock_types, temp_dir / "files",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=temp_dir / "work_files",
        )
        meta_path = harness.write_metadata()
        content = meta_path.read_text()
        lin_line = [l for l in content.split("\n") if l.startswith("lin")][0]
        lin_data = json.loads(lin_line[4:])

        for entry in lin_data:
            files = entry["FILES"]
            assert isinstance(files, list)
            for file_group in files:
                assert isinstance(file_group, list)
                for f in file_group:
                    assert isinstance(f, str)


class TestHarnessErrors:
    def test_failing_transform(self, mock_samples, mock_types, temp_dir):
        task = _make_task(
            mock_samples, mock_types, temp_dir / "fail",
            failing_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=temp_dir / "work_fail",
        )
        result = harness.run()
        assert not result.success

    def test_missing_work_dir_raises(self, mock_samples, mock_types, temp_dir):
        task = _make_task(
            mock_samples, mock_types, temp_dir / "nowork",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=None,
        )
        with pytest.raises(ValueError):
            harness.run()


class TestHarnessBranching:
    def test_multi_product_groups(self, mock_samples, mock_types, temp_dir):
        transforms = branching_transforms()
        tr_lib = create_transform_library(temp_dir / "branch_tr", mock_types, transforms)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
        target_model = Transform()
        target_model.AddRequirement(properties={"merged"})
        target_names = ["merged"]

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )
        assert isinstance(plan, WorkflowPlan)
        assert len(plan.steps) == 3

        task = WorkflowTask(
            ok=True,
            plan=plan,
            data_libraries=[mock_samples],
            transform_libraries=[tr_lib],
        )

        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=temp_dir / "work_branch",
        )
        result = harness.run()
        assert result.success
