import pytest
import time
from pathlib import Path
from metasmith.python_api import (
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Resources,
    Size,
)

from conftest import (
    wait_for_workflow,
    verify_fasta_output,
    verify_json_output,
    MLIB,
    TEST_DATA_DIR,
)


@pytest.fixture(scope="module")
def assembly_transforms(mlib):
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/assembly"),
    ]


@pytest.fixture
def short_reads_input(tmp_inputs, test_data_dir):
    inputs = tmp_inputs(["sequences.yml"])

    reads_path = test_data_dir / "small_reads.fq.gz"
    if not reads_path.exists():
        pytest.skip("Test data not available: small_reads.fq.gz")

    meta = inputs.AddValue(
        "reads_metadata.json",
        {"parity": "single", "length_class": "short"},
        "sequences::read_metadata",
    )
    inputs.AddItem(reads_path, "sequences::short_reads_se", parents={meta})
    inputs.LocalizeContents()
    inputs.Save()

    return inputs


@pytest.fixture
def long_reads_input(tmp_inputs, test_data_dir):
    inputs = tmp_inputs(["sequences.yml"])

    reads_path = test_data_dir / "small_long_reads.fq.gz"
    if not reads_path.exists():
        pytest.skip("Test data not available: small_long_reads.fq.gz")

    meta = inputs.AddValue(
        "reads_metadata.json",
        {"parity": "single", "length_class": "long"},
        "sequences::read_metadata",
    )
    inputs.AddItem(reads_path, "sequences::long_reads", parents={meta})
    inputs.LocalizeContents()
    inputs.Save()

    return inputs


class TestAssemblyWorkflowGeneration:
    def test_can_plan_read_qc_workflow(
        self, agent, base_resources, assembly_transforms, short_reads_input
    ):
        targets = TargetBuilder()
        targets.Add("sequences::read_qc_stats")

        task = agent.GenerateWorkflow(
            samples=list(short_reads_input.AsSamples("sequences::read_metadata")),
            resources=base_resources + [short_reads_input],
            transforms=assembly_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0, "Workflow should have at least one step"

    def test_can_plan_megahit_assembly_workflow(
        self, agent, base_resources, assembly_transforms, short_reads_input
    ):
        targets = TargetBuilder()
        targets.Add("sequences::assembly")

        task = agent.GenerateWorkflow(
            samples=list(short_reads_input.AsSamples("sequences::read_metadata")),
            resources=base_resources + [short_reads_input],
            transforms=assembly_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"

    def test_can_plan_assembly_stats_workflow(
        self, agent, base_resources, assembly_transforms, short_reads_input
    ):
        targets = TargetBuilder()
        targets.Add("sequences::assembly_stats")

        task = agent.GenerateWorkflow(
            samples=list(short_reads_input.AsSamples("sequences::read_metadata")),
            resources=base_resources + [short_reads_input],
            transforms=assembly_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"


@pytest.mark.slow
class TestAssemblyWorkflowExecution:
    def test_read_qc_e2e(
        self, agent, base_resources, assembly_transforms, short_reads_input, tmp_path
    ):
        targets = TargetBuilder()
        targets.Add("sequences::read_qc_stats")

        task = agent.GenerateWorkflow(
            samples=list(short_reads_input.AsSamples("sequences::read_metadata")),
            resources=base_resources + [short_reads_input],
            transforms=assembly_transforms,
            targets=targets,
        )
        assert task.ok, f"Workflow generation failed: {task}"

        agent.StageWorkflow(task, on_exist="clear")

        agent.RunWorkflow(
            task,
            config_file=agent.GetNxfConfigPresets()["local"],
            params=dict(
                executor=dict(cpus=4, queueSize=1),
                process=dict(tries=1),
            ),
        )

        results = wait_for_workflow(agent, task, timeout=300)

        found_stats = False
        for path, type_name, endpoint in results.Iterate():
            if "read_qc_stats" in type_name:
                found_stats = True
                results_path = agent.GetResultSource(task).GetPath()
                full_path = results_path / path
                assert verify_json_output(full_path), f"Invalid JSON: {full_path}"

        assert found_stats, "No read_qc_stats output found"

    def test_assembly_e2e(
        self, agent, base_resources, assembly_transforms, short_reads_input, tmp_path
    ):
        targets = TargetBuilder()
        targets.Add("sequences::assembly")

        task = agent.GenerateWorkflow(
            samples=list(short_reads_input.AsSamples("sequences::read_metadata")),
            resources=base_resources + [short_reads_input],
            transforms=assembly_transforms,
            targets=targets,
        )
        assert task.ok, f"Workflow generation failed: {task}"

        agent.StageWorkflow(task, on_exist="clear")
        agent.RunWorkflow(
            task,
            config_file=agent.GetNxfConfigPresets()["local"],
            params=dict(
                executor=dict(cpus=4, queueSize=1),
                process=dict(tries=1),
            ),
        )

        results = wait_for_workflow(agent, task, timeout=600)

        found_assembly = False
        results_path = agent.GetResultSource(task).GetPath()
        for path, type_name, endpoint in results.Iterate():
            if "assembly" in type_name and "stats" not in type_name:
                found_assembly = True
                full_path = results_path / path
                if not path.is_absolute():
                    assert verify_fasta_output(full_path), f"Invalid FASTA: {full_path}"

        assert found_assembly, "No assembly output found"
