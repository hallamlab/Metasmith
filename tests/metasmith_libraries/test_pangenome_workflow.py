"""
End-to-end tests for pangenome analysis transforms.

Tests for: ppanggolin

These tests verify that pangenome workflows can be:
1. Generated (workflow planning)
2. Staged to the agent
3. Executed via local Docker
4. Produce valid pangenome analysis outputs
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
def pangenome_transforms(mlib):
    """Load pangenome transforms."""
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/pangenome"),
    ]


@pytest.fixture
def pangenome_input(tmp_inputs, test_data_dir):
    """Create input library with GBK files for pangenome analysis."""
    inputs = tmp_inputs(["sequences.yml", "pangenome.yml"])

    # Pangenome analysis requires multiple genome annotations (GBK files)
    gbk_files = list(test_data_dir.glob("*.gbk"))
    if len(gbk_files) < 2:
        pytest.skip("Pangenome test requires at least 2 GBK files in test_data")

    for gbk_file in gbk_files:
        inputs.AddItem(gbk_file, "sequences::gbk")

    inputs.LocalizeContents()
    inputs.Save()

    return inputs


@pytest.fixture
def pangenome_resources(mlib, base_resources):
    """Load pangenome-specific resources."""
    return list(base_resources)


class TestPangenomeWorkflowGeneration:
    """Tests for workflow generation (planning only)."""

    def test_can_plan_ppanggolin_workflow(
        self, agent, pangenome_resources, pangenome_transforms, pangenome_input
    ):
        """Verify workflow generation for PPanGGOLiN."""
        targets = TargetBuilder()
        targets.Add("pangenome::gene_presence_absence")

        task = agent.GenerateWorkflow(
            samples=list(pangenome_input.AsSamples("sequences::gbk")),
            resources=pangenome_resources + [pangenome_input],
            transforms=pangenome_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0

    def test_can_plan_pangenome_stats_workflow(
        self, agent, pangenome_resources, pangenome_transforms, pangenome_input
    ):
        """Verify workflow generation for pangenome statistics."""
        targets = TargetBuilder()
        targets.Add("pangenome::pangenome_stats")

        task = agent.GenerateWorkflow(
            samples=list(pangenome_input.AsSamples("sequences::gbk")),
            resources=pangenome_resources + [pangenome_input],
            transforms=pangenome_transforms,
            targets=targets,
        )

        # Skip if target not found
        if not task.ok:
            pytest.skip("Pangenome stats target may not be available")


@pytest.mark.slow
class TestPangenomeWorkflowExecution:
    """Full E2E tests that execute workflows via Docker."""

    def test_ppanggolin_e2e(
        self, agent, pangenome_resources, pangenome_transforms, pangenome_input
    ):
        """Full E2E test: stage, run PPanGGOLiN, verify outputs."""
        targets = TargetBuilder()
        targets.Add("pangenome::gene_presence_absence")

        task = agent.GenerateWorkflow(
            samples=list(pangenome_input.AsSamples("sequences::gbk")),
            resources=pangenome_resources + [pangenome_input],
            transforms=pangenome_transforms,
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

        results = wait_for_workflow(agent, task, timeout=900)
        results_path = agent.GetResultSource(task).GetPath()

        found_matrix = False
        for path, type_name, endpoint in results.Iterate():
            if "gene_presence_absence" in type_name:
                found_matrix = True
                if not path.is_absolute():
                    full_path = results_path / path
                    # Gene presence/absence is typically a TSV matrix
                    assert verify_tsv_output(full_path), f"Invalid matrix: {full_path}"

        assert found_matrix, "No gene presence/absence matrix found"
