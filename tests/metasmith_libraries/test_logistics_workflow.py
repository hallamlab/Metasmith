"""
End-to-end tests for logistics transforms.

Tests for: getNcbiAssembly, getNcbiSra

These tests verify that data retrieval workflows can be:
1. Generated (workflow planning)
2. Staged to the agent
3. Executed via local Docker
4. Successfully download data from NCBI
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
    MLIB,
    TEST_DATA_DIR,
)


@pytest.fixture(scope="module")
def logistics_transforms(mlib):
    """Load logistics transforms."""
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/logistics"),
    ]


@pytest.fixture
def sra_input(tmp_inputs):
    """Create input library with SRA accession."""
    inputs = tmp_inputs(["ncbi.yml", "sequences.yml"])

    # Use a small SRA accession for testing
    # SRR5585544 is a small dataset
    meta = inputs.AddValue(
        "reads_metadata.json",
        {"parity": "single", "length_class": "short"},
        "sequences::read_metadata",
    )
    inputs.AddValue(
        "SRR5585544.acc",
        "SRR5585544",
        "ncbi::sra_accession",
        parents={meta},
    )

    inputs.Save()
    return inputs


@pytest.fixture
def assembly_accession_input(tmp_inputs):
    """Create input library with NCBI assembly accession."""
    inputs = tmp_inputs(["ncbi.yml", "sequences.yml"])

    # Use a small bacterial assembly for testing. The accession descends from
    # the name it is fetched under -- getNcbiAssembly declares that, so
    # everything it produces carries the name in its lineage.
    name = inputs.AddValue(
        "GCF_000005845.name",
        "K12",
        "ncbi::genome_name",
    )
    inputs.AddValue(
        "GCF_000005845.acc",
        "GCF_000005845.2",  # E. coli K-12 reference
        "ncbi::assembly_accession",
        parents={name},
    )

    inputs.Save()
    return inputs


class TestLogisticsWorkflowGeneration:
    """Tests for workflow generation (planning only)."""

    def test_can_plan_sra_download_workflow(
        self, agent, base_resources, logistics_transforms, sra_input
    ):
        """Verify workflow generation for SRA download."""
        targets = TargetBuilder()
        targets.Add("sequences::reads")

        task = agent.GenerateWorkflow(
            samples=list(sra_input.AsSamples("sequences::read_metadata")),
            resources=base_resources + [sra_input],
            transforms=logistics_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0

    def test_can_plan_assembly_download_workflow(
        self, agent, base_resources, logistics_transforms, assembly_accession_input
    ):
        """Verify workflow generation for NCBI assembly download."""
        targets = TargetBuilder()
        targets.Add("sequences::assembly")

        task = agent.GenerateWorkflow(
            samples=list(assembly_accession_input.AsSamples("ncbi::assembly_accession")),
            resources=base_resources + [assembly_accession_input],
            transforms=logistics_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"


@pytest.mark.slow
@pytest.mark.network
class TestLogisticsWorkflowExecution:
    """
    Full E2E tests that execute workflows via Docker.

    These tests require network access to NCBI.
    """

    def test_sra_download_e2e(
        self, agent, base_resources, logistics_transforms, sra_input
    ):
        """Full E2E test: stage, run SRA download, verify reads."""
        targets = TargetBuilder()
        targets.Add("sequences::reads")

        task = agent.GenerateWorkflow(
            samples=list(sra_input.AsSamples("sequences::read_metadata")),
            resources=base_resources + [sra_input],
            transforms=logistics_transforms,
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

        # SRA downloads can take a while
        results = wait_for_workflow(agent, task, timeout=1200)
        results_path = agent.GetResultSource(task).GetPath()

        found_reads = False
        for path, type_name, endpoint in results.Iterate():
            if "reads" in type_name:
                found_reads = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert full_path.exists(), f"Reads file missing: {full_path}"

        assert found_reads, "No reads downloaded from SRA"

    def test_assembly_download_e2e(
        self, agent, base_resources, logistics_transforms, assembly_accession_input
    ):
        """Full E2E test: stage, run assembly download, verify FASTA."""
        targets = TargetBuilder()
        targets.Add("sequences::assembly")

        task = agent.GenerateWorkflow(
            samples=list(assembly_accession_input.AsSamples("ncbi::assembly_accession")),
            resources=base_resources + [assembly_accession_input],
            transforms=logistics_transforms,
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
        results_path = agent.GetResultSource(task).GetPath()

        found_assembly = False
        for path, type_name, endpoint in results.Iterate():
            if "assembly" in type_name:
                found_assembly = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert verify_fasta_output(full_path), f"Invalid FASTA: {full_path}"

        assert found_assembly, "No assembly downloaded from NCBI"
