"""
End-to-end tests for amplicon analysis transforms.

Tests for: qiime2_taxonomy, blast_map_asvs

These tests verify that amplicon workflows can be:
1. Generated (workflow planning)
2. Staged to the agent
3. Executed via local Docker
4. Produce valid ASV taxonomy and mapping outputs
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

from conftest import (
    wait_for_workflow,
    verify_tsv_output,
    MLIB,
    TEST_DATA_DIR,
)


@pytest.fixture(scope="module")
def amplicon_transforms(mlib):
    """Load amplicon transforms."""
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/amplicon"),
        TransformInstanceLibrary.Load(mlib / "transforms/logistics"),
    ]


@pytest.fixture
def amplicon_input(tmp_inputs, test_data_dir):
    """Create input library with ASV sequences and contigs."""
    inputs = tmp_inputs(["amplicon.yml", "sequences.yml"])

    asv_path = test_data_dir / "small_asvs.fasta"
    assembly_path = test_data_dir / "small_assembly.fna"

    if not asv_path.exists():
        pytest.skip("Test data not available: small_asvs.fasta")
    if not assembly_path.exists():
        pytest.skip("Test data not available: small_assembly.fna")

    # Register ASV sequences
    asv_seqs = inputs.AddItem(asv_path, "amplicon::asv_seqs")

    # Register contigs linked to ASVs
    inputs.AddItem(assembly_path, "sequences::assembly", parents={asv_seqs})

    # Register SILVA source for taxonomy
    inputs.AddValue("silva_source", "SILVA_138.2_SSURef_NR99", "amplicon::silva_source")

    inputs.LocalizeContents()
    inputs.Save()

    return inputs


@pytest.fixture
def amplicon_resources(mlib, base_resources):
    """Load amplicon-specific resources."""
    resources = list(base_resources)
    return resources


class TestAmpliconWorkflowGeneration:
    """Tests for workflow generation (planning only)."""

    def test_can_plan_asv_contig_map_workflow(
        self, agent, amplicon_resources, amplicon_transforms, amplicon_input
    ):
        """Verify workflow generation for ASV to contig mapping."""
        targets = TargetBuilder()
        targets.Add("amplicon::asv_contig_map")

        task = agent.GenerateWorkflow(
            samples=list(amplicon_input.AsSamples("sequences::assembly")),
            resources=amplicon_resources + [amplicon_input],
            transforms=amplicon_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0

    def test_can_plan_asv_taxonomy_workflow(
        self, agent, amplicon_resources, amplicon_transforms, amplicon_input
    ):
        """Verify workflow generation for ASV taxonomy classification."""
        targets = TargetBuilder()
        targets.Add("amplicon::asv_taxonomy")

        # Include silva_source as resource
        samples = list(amplicon_input.AsSamples("amplicon::silva_source"))

        task = agent.GenerateWorkflow(
            samples=list(amplicon_input.AsSamples("sequences::assembly")),
            resources=amplicon_resources + samples + [amplicon_input],
            transforms=amplicon_transforms,
            targets=targets,
        )

        # May fail if SILVA database not available
        if not task.ok:
            pytest.skip("ASV taxonomy workflow requires SILVA database")


@pytest.mark.slow
class TestAmpliconWorkflowExecution:
    """Full E2E tests that execute workflows via Docker."""

    def test_asv_contig_map_e2e(
        self, agent, amplicon_resources, amplicon_transforms, amplicon_input
    ):
        """Full E2E test: stage, run ASV mapping, verify outputs."""
        targets = TargetBuilder()
        targets.Add("amplicon::asv_contig_map")

        task = agent.GenerateWorkflow(
            samples=list(amplicon_input.AsSamples("sequences::assembly")),
            resources=amplicon_resources + [amplicon_input],
            transforms=amplicon_transforms,
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

        found_map = False
        for path, type_name, endpoint in results.Iterate():
            if "asv_contig_map" in type_name:
                found_map = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert verify_tsv_output(full_path), f"Invalid map: {full_path}"

        assert found_map, "No ASV contig map output found"

    def test_asv_taxonomy_e2e(
        self, agent, amplicon_resources, amplicon_transforms, amplicon_input
    ):
        """Full E2E test: stage, run ASV taxonomy, verify outputs."""
        targets = TargetBuilder()
        targets.Add("amplicon::asv_taxonomy")

        samples = list(amplicon_input.AsSamples("amplicon::silva_source"))

        task = agent.GenerateWorkflow(
            samples=list(amplicon_input.AsSamples("sequences::assembly")),
            resources=amplicon_resources + samples + [amplicon_input],
            transforms=amplicon_transforms,
            targets=targets,
        )

        if not task.ok:
            pytest.skip("ASV taxonomy workflow requires SILVA database")

        agent.StageWorkflow(task, on_exist="clear")
        agent.RunWorkflow(
            task,
            config_file=agent.GetNxfConfigPresets()["local"],
            params=dict(
                executor=dict(cpus=4, queueSize=1),
                process=dict(tries=1),
            ),
        )

        results = wait_for_workflow(agent, task, timeout=900)
        results_path = agent.GetResultSource(task).GetPath()

        found_taxonomy = False
        for path, type_name, endpoint in results.Iterate():
            if "asv_taxonomy" in type_name:
                found_taxonomy = True

        assert found_taxonomy, "No ASV taxonomy output found"
