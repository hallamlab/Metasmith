"""
End-to-end tests for transcriptomics transforms (salmon index, quant, count table).

These tests verify that transcriptomics workflows can be:
1. Generated (workflow planning)
2. Staged to the agent
3. Executed via local Docker
4. Produce valid output files
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
    verify_tsv_output,
    MLIB,
    TEST_DATA_DIR,
)


PORPHYRIDIUM_READS_DIR = TEST_DATA_DIR / "porphyridium" / "Raw-transcriptome-data"
PORPHYRIDIUM_ACCESSION = "GCA_008690995.1"

SAMPLES = [
    "POR-0-1", "POR-0-2", "POR-0-3",
    "POR-S-1", "POR-S-2", "POR-S-3",
    "POR-8-1", "POR-8-2", "POR-8-3",
]

# Map sample name prefix to R1/R2 file patterns
SAMPLE_FILES = {
    "POR-0-1": ("POR-0-1-090325_S59_L001_R1_001.fastq.gz", "POR-0-1-090325_S59_L001_R2_001.fastq.gz"),
    "POR-0-2": ("POR-0-2-090325_S60_L001_R1_001.fastq.gz", "POR-0-2-090325_S60_L001_R2_001.fastq.gz"),
    "POR-0-3": ("POR-0-3-090325_S61_L001_R1_001.fastq.gz", "POR-0-3-090325_S61_L001_R2_001.fastq.gz"),
    "POR-S-1": ("POR-S-1-090325_S62_L001_R1_001.fastq.gz", "POR-S-1-090325_S62_L001_R2_001.fastq.gz"),
    "POR-S-2": ("POR-S-2-090325_S67_L001_R1_001.fastq.gz", "POR-S-2-090325_S67_L001_R2_001.fastq.gz"),
    "POR-S-3": ("POR-S-3-090325_S68_L001_R1_001.fastq.gz", "POR-S-3-090325_S68_L001_R2_001.fastq.gz"),
    "POR-8-1": ("POR-8-1-090325_S66_L001_R1_001.fastq.gz", "POR-8-1-090325_S66_L001_R2_001.fastq.gz"),
    "POR-8-2": ("POR-8-2-090325_S69_L001_R1_001.fastq.gz", "POR-8-2-090325_S69_L001_R2_001.fastq.gz"),
    "POR-8-3": ("POR-8-3-090325_S70_L001_R1_001.fastq.gz", "POR-8-3-090325_S70_L001_R2_001.fastq.gz"),
}


@pytest.fixture(scope="module")
def transcriptomics_transforms(mlib):
    """Load transcriptomics and logistics transforms."""
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/transcriptomics"),
        TransformInstanceLibrary.Load(mlib / "transforms/logistics"),
    ]


@pytest.fixture
def porphyridium_input(tmp_inputs, test_data_dir):
    """Create input library with Porphyridium paired-end reads and reference accession."""
    if not PORPHYRIDIUM_READS_DIR.exists():
        pytest.skip("Porphyridium test data not available")

    inputs = tmp_inputs(["sequences.yml", "ncbi.yml", "transcriptomics.yml"])

    # Add the experiment grouping marker
    experiment = inputs.AddValue(
        "porphyridium_experiment.txt",
        "porphyridium_transcriptomics",
        "transcriptomics::experiment",
    )

    # Add reference assembly accession
    accession = inputs.AddValue(
        "porphyridium_accession.txt",
        PORPHYRIDIUM_ACCESSION,
        "ncbi::assembly_accession",
        parents={experiment},
    )

    # Add paired-end reads for each sample
    for sample_name in SAMPLES:
        r1_file, r2_file = SAMPLE_FILES[sample_name]
        r1_path = PORPHYRIDIUM_READS_DIR / r1_file
        r2_path = PORPHYRIDIUM_READS_DIR / r2_file

        if not r1_path.exists() or not r2_path.exists():
            pytest.skip(f"Missing read files for {sample_name}")

        pair = inputs.AddValue(
            f"{sample_name}_pair.txt",
            sample_name,
            "sequences::read_pair",
            parents={experiment},
        )
        inputs.AddItem(r1_path, "sequences::zipped_forward_short_reads", parents={pair})
        inputs.AddItem(r2_path, "sequences::zipped_reverse_short_reads", parents={pair})

    inputs.LocalizeContents()
    inputs.Save()

    return inputs


class TestWorkflowGeneration:
    """Tests for workflow generation (planning only, no execution)."""

    def test_can_plan_salmon_quant(
        self, agent, base_resources, transcriptomics_transforms, porphyridium_input
    ):
        """Verify workflow generation for salmon quantification."""
        targets = TargetBuilder()
        targets.Add("transcriptomics::salmon_quant")

        task = agent.GenerateWorkflow(
            samples=list(porphyridium_input.AsSamples("transcriptomics::experiment")),
            resources=base_resources + [porphyridium_input],
            transforms=transcriptomics_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0, "Workflow should have at least one step"

    def test_can_plan_count_table(
        self, agent, base_resources, transcriptomics_transforms, porphyridium_input
    ):
        """Verify workflow generation for count table."""
        targets = TargetBuilder()
        targets.Add("transcriptomics::count_table")

        task = agent.GenerateWorkflow(
            samples=list(porphyridium_input.AsSamples("transcriptomics::experiment")),
            resources=base_resources + [porphyridium_input],
            transforms=transcriptomics_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0, "Workflow should have at least one step"


@pytest.mark.slow
class TestWorkflowExecution:
    """Full E2E tests that execute workflows via Docker."""

    def test_salmon_quant_e2e(
        self, agent, base_resources, transcriptomics_transforms, porphyridium_input, tmp_path
    ):
        """Full E2E test: generate, stage, run salmon quant, verify outputs."""
        targets = TargetBuilder()
        targets.Add("transcriptomics::salmon_quant")

        task = agent.GenerateWorkflow(
            samples=list(porphyridium_input.AsSamples("transcriptomics::experiment")),
            resources=base_resources + [porphyridium_input],
            transforms=transcriptomics_transforms,
            targets=targets,
        )
        assert task.ok, f"Workflow generation failed: {task}"

        agent.StageWorkflow(task, on_exist="clear")
        agent.RunWorkflow(
            task,
            config_file=agent.GetNxfConfigPresets()["local"],
            params=dict(
                executor=dict(cpus=8, queueSize=1),
                process=dict(tries=1),
            ),
        )

        results = wait_for_workflow(agent, task, timeout=7200)

        found_quant = False
        results_path = agent.GetResultSource(task).GetPath()
        for path, type_name, endpoint in results.Iterate():
            if "salmon_quant" in type_name:
                found_quant = True
                full_path = path if path.is_absolute() else results_path / path
                assert full_path.exists(), f"Quant file missing: {full_path}"

        assert found_quant, "No salmon_quant output found"

    def test_count_table_e2e(
        self, agent, base_resources, transcriptomics_transforms, porphyridium_input, tmp_path
    ):
        """Full E2E test: generate, stage, run full pipeline, verify count table."""
        targets = TargetBuilder()
        targets.Add("transcriptomics::count_table")

        task = agent.GenerateWorkflow(
            samples=list(porphyridium_input.AsSamples("transcriptomics::experiment")),
            resources=base_resources + [porphyridium_input],
            transforms=transcriptomics_transforms,
            targets=targets,
        )
        assert task.ok, f"Workflow generation failed: {task}"

        agent.StageWorkflow(task, on_exist="clear")
        agent.RunWorkflow(
            task,
            config_file=agent.GetNxfConfigPresets()["local"],
            params=dict(
                executor=dict(cpus=8, queueSize=1),
                process=dict(tries=1),
            ),
        )

        results = wait_for_workflow(agent, task, timeout=7200)

        found_table = False
        results_path = agent.GetResultSource(task).GetPath()
        for path, type_name, endpoint in results.Iterate():
            if "count_table" in type_name:
                found_table = True
                full_path = path if path.is_absolute() else results_path / path
                assert verify_tsv_output(full_path, expected_headers=["Name", "Length"]), \
                    f"Invalid count table: {full_path}"

        assert found_table, "No count_table output found"
