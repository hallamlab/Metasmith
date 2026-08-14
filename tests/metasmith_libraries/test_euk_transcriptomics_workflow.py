"""
End-to-end tests for eukaryotic transcriptomics transforms
(STAR index, STAR align, StringTie assemble/merge/quant, gene count matrix).

These tests verify that the eukaryotic transcriptomics pipeline can be:
1. Generated (workflow planning)
2. Executed via local Docker to produce a gene-level count matrix
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


PORPHYRIDIUM_READS_DIR = TEST_DATA_DIR / "porphyridium" / "Raw-transcriptome-data"
PORPHYRIDIUM_ACCESSION = "GCA_008690995.1"

SAMPLES = [
    "POR-0-1", "POR-0-2", "POR-0-3",
    "POR-S-1", "POR-S-2", "POR-S-3",
    "POR-8-1", "POR-8-2", "POR-8-3",
]

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
def euk_transcriptomics_transforms(mlib):
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

    # Add reference assembly accession, under the name it is fetched as.
    reference_name = inputs.AddValue(
        "porphyridium_name.txt",
        "porphyridium",
        "ncbi::genome_name",
        parents={experiment},
    )
    accession = inputs.AddValue(
        "porphyridium_accession.txt",
        PORPHYRIDIUM_ACCESSION,
        "ncbi::assembly_accession",
        parents={reference_name},
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

    def test_can_plan_gene_count_table(
        self, agent, base_resources, euk_transcriptomics_transforms, porphyridium_input
    ):
        """Verify workflow generation for eukaryotic gene count table includes all pipeline steps."""
        targets = TargetBuilder()
        targets.Add("transcriptomics::gene_count_table")

        task = agent.GenerateWorkflow(
            samples=list(porphyridium_input.AsSamples("transcriptomics::experiment")),
            resources=base_resources + [porphyridium_input],
            transforms=euk_transcriptomics_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0, "Workflow should have at least one step"

        # The pipeline should have at least 7 steps:
        # getNcbiAssembly, star_genome_index, star_align, stringtie_assemble,
        # stringtie_merge, stringtie_quant, gene_count_matrix
        assert len(task.plan.steps) >= 7, \
            f"Expected at least 7 steps, got {len(task.plan.steps)}"

    def test_can_plan_diff_count_table(
        self, agent, base_resources, euk_transcriptomics_transforms, porphyridium_input
    ):
        """Verify workflow generation for PyDESeq2 normalized count table."""
        targets = TargetBuilder()
        targets.Add("transcriptomics::diff_count_table")

        task = agent.GenerateWorkflow(
            samples=list(porphyridium_input.AsSamples("transcriptomics::experiment")),
            resources=base_resources + [porphyridium_input],
            transforms=euk_transcriptomics_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0, "Workflow should have at least one step"

        # The pipeline should have at least 7 steps:
        # getNcbiAssembly, star_genome_index, star_align, stringtie_assemble,
        # stringtie_merge, stringtie_quant, pydeseq2
        assert len(task.plan.steps) >= 7, \
            f"Expected at least 7 steps, got {len(task.plan.steps)}"


@pytest.mark.slow
class TestWorkflowExecution:
    """Full E2E tests that execute workflows via Docker."""

    def test_gene_count_table_e2e(
        self, agent, base_resources, euk_transcriptomics_transforms, porphyridium_input, tmp_path
    ):
        """Full E2E test: run eukaryotic pipeline, verify gene-level count CSV."""
        targets = TargetBuilder()
        targets.Add("transcriptomics::gene_count_table")

        task = agent.GenerateWorkflow(
            samples=list(porphyridium_input.AsSamples("transcriptomics::experiment")),
            resources=base_resources + [porphyridium_input],
            transforms=euk_transcriptomics_transforms,
            targets=targets,
        )
        assert task.ok, f"Workflow generation failed: {task}"

        agent.StageWorkflow(task, on_exist="clear")
        agent.RunWorkflow(
            task,
            config_file=agent.GetNxfConfigPresets()["local"],
            params=dict(
                executor=dict(cpus=14, memory="32 GB", queueSize=1),
                process=dict(tries=1),
            ),
        )

        results = wait_for_workflow(agent, task, timeout=86400)

        found_table = False
        results_path = agent.GetResultSource(task).GetPath()
        for path, type_name, endpoint in results.Iterate():
            if "gene_count_table" in type_name:
                found_table = True
                full_path = path if path.is_absolute() else results_path / path
                assert full_path.exists(), f"Gene count table missing: {full_path}"

                # Verify CSV structure
                content = full_path.read_text()
                lines = content.strip().split("\n")
                assert len(lines) > 1, "Gene count table should have header + data rows"

                header = lines[0].split(",")
                assert header[0] == "gene_id", f"Expected 'gene_id' column, got: {header[0]}"
                assert len(header) > 1, "Gene count table should have sample columns"

        assert found_table, "No gene_count_table output found"
