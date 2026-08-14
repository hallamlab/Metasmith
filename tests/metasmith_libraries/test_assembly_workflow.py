"""
End-to-end tests for assembly transforms (bbduk, megahit, flye, etc.)

These tests verify that assembly workflows can be:
1. Generated (workflow planning)
2. Staged to the agent
3. Executed via local Docker
4. Produce valid output files
"""
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

from tests.conftest import (
    wait_for_workflow,
    verify_fasta_output,
    verify_json_output,
    MLIB,
    TEST_DATA_DIR,
)


@pytest.fixture(scope="module")
def assembly_transforms(mlib):
    """Load assembly transforms."""
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/assembly"),
    ]


@pytest.fixture
def short_reads_input(tmp_inputs, test_data_dir):
    """Create input library with short reads."""
    inputs = tmp_inputs(["sequences.yml"])

    # Check if test data exists, create minimal if not
    reads_path = test_data_dir / "small_reads.fq.gz"
    if not reads_path.exists():
        pytest.skip("Test data not available: small_reads.fq.gz")

    # Add read metadata and reads
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
    """Create input library with long reads."""
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
    """Tests for workflow generation (planning only, no execution)."""

    def test_can_plan_read_qc_workflow(
        self, agent, base_resources, assembly_transforms, short_reads_input
    ):
        """Verify workflow generation for read QC stats."""
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
        """Verify workflow generation for MEGAHIT assembly."""
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
        """Verify workflow generation for assembly with stats."""
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
    """Full E2E tests that execute workflows via Docker."""

    def test_read_qc_e2e(
        self, agent, base_resources, assembly_transforms, short_reads_input, tmp_path
    ):
        """Full E2E test: generate, stage, run read QC, verify outputs."""
        targets = TargetBuilder()
        targets.Add("sequences::read_qc_stats")

        task = agent.GenerateWorkflow(
            samples=list(short_reads_input.AsSamples("sequences::read_metadata")),
            resources=base_resources + [short_reads_input],
            transforms=assembly_transforms,
            targets=targets,
        )
        assert task.ok, f"Workflow generation failed: {task}"

        # Stage workflow
        agent.StageWorkflow(task, on_exist="clear")

        # Run workflow
        agent.RunWorkflow(
            task,
            config_file=agent.GetNxfConfigPresets()["local"],
            params=dict(
                executor=dict(cpus=4, queueSize=1),
                process=dict(tries=1),
            ),
        )

        # Wait for completion and verify
        results = wait_for_workflow(agent, task, timeout=300)

        # Check outputs
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
        """Full E2E test: generate, stage, run assembly, verify FASTA output."""
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
