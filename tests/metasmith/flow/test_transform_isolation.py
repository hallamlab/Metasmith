from metasmith.models.workflow import Case
import pytest
from pathlib import Path

from metasmith.models.libraries import (
    DataInstanceLibrary,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.lineage import LinPayload
from metasmith.models.workflow import WorkflowPlan, WorkflowTask, METADATA_FILE
from metasmith.testing.transform_harness import TransformHarness, MockShell
from metasmith.testing.mock_transforms import (
    alignment_transform,
    batched_transform,
    branching_transforms,
    failing_transform,
)

from tests.metasmith.support import create_transform_library


def lin_line_of(metadata: str) -> str:
    """The `lin` line's payload, which is an envelope rather than a bare list."""
    line = [l for l in metadata.split("\n") if l.startswith("lin ")][0]
    return line[4:]


def _make_task(mock_samples, mock_types, tmp_path, transforms, target_props, target_name):
    tr_lib = create_transform_library(tmp_path, mock_types, transforms)
    given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
    target_model = Transform()
    target_model.AddRequirement(properties=target_props)
    target_names = [target_name]

    plan = WorkflowPlan.Generate(
        cases=Case.ByShape(given, target=target_model, target_names=target_names),
        transforms=[tr_lib],
    )
    assert isinstance(plan, WorkflowPlan)
    return WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=[mock_samples],
        transform_libraries=[tr_lib],
    )


class TestHarnessBasic:
    def test_simple_transform_produces_output(self, mock_samples, mock_types, tmp_path):
        task = _make_task(
            mock_samples, mock_types, tmp_path / "basic",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=tmp_path / "work_basic",
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

    def test_metadata_format(self, mock_samples, mock_types, tmp_path):
        task = _make_task(
            mock_samples, mock_types, tmp_path / "meta",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=tmp_path / "work_meta",
        )
        meta_path = harness.write_metadata()
        assert meta_path.exists()

        content = meta_path.read_text()
        lines = content.strip().split("\n")
        assert len(lines) >= 8

        keys = [l.split(" ", maxsplit=1)[0] for l in lines]
        for expected in ["res", "lin", "fmt", "din", "dot", "inp", "out"]:
            assert expected in keys

        assert isinstance(LinPayload.from_json(lin_line_of(content)).entries, list)

    def test_input_files_accessible(self, mock_samples, mock_types, tmp_path):
        task = _make_task(
            mock_samples, mock_types, tmp_path / "inp",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=tmp_path / "work_inp",
        )
        input_map = harness.setup_inputs()
        assert len(input_map) > 0

        for dep, paths in input_map.items():
            for p in paths:
                assert p.exists() or p.is_symlink(), f"Input {p} should be accessible"

    def test_output_path_convention(self, mock_samples, mock_types, tmp_path):
        task = _make_task(
            mock_samples, mock_types, tmp_path / "outpath",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=tmp_path / "work_outpath",
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
    def test_batched_context_iterates(self, mock_samples, mock_types, tmp_path):
        task = _make_task(
            mock_samples, mock_types, tmp_path / "batch",
            batched_transform(batch_size=3), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=tmp_path / "work_batch",
        )
        harness.write_metadata()
        context = harness.build_context()

        batch_count = 0
        for _ in context.AsBatch():
            batch_count += 1
        assert batch_count >= 1

    def test_batch_outputs_per_item(self, mock_samples, mock_types, tmp_path):
        task = _make_task(
            mock_samples, mock_types, tmp_path / "batchout",
            batched_transform(batch_size=3), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=tmp_path / "work_batchout",
        )
        result = harness.run()
        assert result.success or len(result.manifest) > 0


class TestHarnessLineage:
    def test_lineage_metadata_correct(self, mock_samples, mock_types, tmp_path):
        task = _make_task(
            mock_samples, mock_types, tmp_path / "lin",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=tmp_path / "work_lin",
        )
        meta_path = harness.write_metadata()
        content = meta_path.read_text()
        entries = LinPayload.from_json(lin_line_of(content)).entries

        assert isinstance(entries, list)
        assert len(entries) > 0

        for entry in entries:
            assert isinstance(entry, dict)
            assert LinPayload.FILES_KEY in entry

    def test_files_entry_correct(self, mock_samples, mock_types, tmp_path):
        task = _make_task(
            mock_samples, mock_types, tmp_path / "files",
            alignment_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=tmp_path / "work_files",
        )
        meta_path = harness.write_metadata()
        content = meta_path.read_text()
        for entry in LinPayload.from_json(lin_line_of(content)).entries:
            files = entry[LinPayload.FILES_KEY]
            assert isinstance(files, list)
            for file_group in files:
                assert isinstance(file_group, list)
                for f in file_group:
                    assert isinstance(f, str)


class TestHarnessErrors:
    def test_failing_transform(self, mock_samples, mock_types, tmp_path):
        task = _make_task(
            mock_samples, mock_types, tmp_path / "fail",
            failing_transform(), {"bam"}, "bam",
        )
        harness = TransformHarness(
            task=task,
            step_index=1,
            work_dir=tmp_path / "work_fail",
        )
        result = harness.run()
        assert not result.success

    def test_missing_work_dir_raises(self, mock_samples, mock_types, tmp_path):
        task = _make_task(
            mock_samples, mock_types, tmp_path / "nowork",
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
    def test_multi_product_groups(self, mock_samples, mock_types, tmp_path):
        transforms = branching_transforms()
        tr_lib = create_transform_library(tmp_path / "branch_tr", mock_types, transforms)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
        target_model = Transform()
        target_model.AddRequirement(properties={"merged"})
        target_names = ["merged"]

        plan = WorkflowPlan.Generate(
            cases=Case.ByShape(given, target=target_model, target_names=target_names),
            transforms=[tr_lib],
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
            work_dir=tmp_path / "work_branch",
        )
        result = harness.run()
        assert result.success
