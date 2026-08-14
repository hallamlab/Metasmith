"""
End-to-end tests for ORA to FASTQ logistics transform.

Tests for: ora2fastq (ORA decompression + interleaving)

These tests verify that ORA workflows can be:
1. Generated (workflow planning)
2. Executed via local Docker
3. Successfully produce interleaved gzipped FASTQ
"""
import pytest
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
def ora_input(tmp_inputs):
    """Create input library with paired ORA reads."""
    inputs = tmp_inputs(["sequences.yml"])

    ora_dir = TEST_DATA_DIR / "ora2fastq"
    r1_path = ora_dir / "S485_S64_L006_R1_001.fastq.ora"
    r2_path = ora_dir / "S485_S64_L006_R2_001.fastq.ora"

    if not r1_path.exists() or not r2_path.exists():
        pytest.skip("ORA test data not available")

    meta = inputs.AddValue(
        "reads_metadata.json",
        {"parity": "paired", "length_class": "short"},
        "sequences::read_metadata",
    )
    pair = inputs.AddValue(
        "read_pair.json",
        {"atomic": "a pair of read files"},
        "sequences::read_pair",
        parents={meta},
    )
    inputs.AddItem(r1_path, "sequences::forward_ora_reads", parents={pair})
    inputs.AddItem(r2_path, "sequences::reverse_ora_reads", parents={pair})

    inputs.Save()
    return inputs


class TestOra2FastqWorkflowGeneration:
    """Tests for workflow generation (planning only)."""

    def test_can_plan_ora2fastq_workflow(
        self, agent, base_resources, logistics_transforms, ora_input
    ):
        """Verify workflow generation for ORA to FASTQ conversion."""
        targets = TargetBuilder()
        targets.Add("sequences::short_reads")

        task = agent.GenerateWorkflow(
            samples=list(ora_input.AsSamples("sequences::read_metadata")),
            resources=base_resources + [ora_input],
            transforms=logistics_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0


@pytest.mark.slow
class TestOra2FastqWorkflowExecution:
    """
    Full E2E tests that execute workflows via Docker.

    These tests require the orad and bbtools containers.
    """

    def test_ora2fastq_e2e(
        self, agent, base_resources, logistics_transforms, ora_input
    ):
        """Full E2E test: decompress ORA, interleave, verify FASTQ output."""
        targets = TargetBuilder()
        targets.Add("sequences::short_reads")

        task = agent.GenerateWorkflow(
            samples=list(ora_input.AsSamples("sequences::read_metadata")),
            resources=base_resources + [ora_input],
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

        found_reads = False
        for path, type_name, endpoint in results.Iterate():
            if "short_reads" in type_name:
                found_reads = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert full_path.exists(), f"Reads file missing: {full_path}"
                    assert full_path.stat().st_size > 0, f"Reads file empty: {full_path}"

        assert found_reads, "No short_reads produced from ORA conversion"
