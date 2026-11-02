import pytest
from pathlib import Path

from metasmith.python_api import (
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
)

from conftest import MLIB


@pytest.fixture(scope="module")
def all_transforms():
    return [
        TransformInstanceLibrary.Load(MLIB / f"transforms/{d}")
        for d in (
            "assembly", "metagenomics", "functionalAnnotation",
            "logistics", "pangenome",
        )
    ]


@pytest.fixture
def meta_assembly_with_bam(tmp_inputs):
    inputs = tmp_inputs(["sequences.yml", "alignment.yml", "binning.yml", "taxonomy.yml"])
    fake_asm = MLIB / "tests" / "test_data" / "fake_hifiasm_meta.fna"
    fake_bam = MLIB / "tests" / "test_data" / "fake.bam"
    asm = inputs.AddItem(fake_asm, "sequences::hifiasm_meta_assembly")
    inputs.AddItem(fake_bam, "alignment::bam", parents={asm})
    inputs.Save()
    return inputs


@pytest.fixture
def bin_only_input(tmp_inputs):
    inputs = tmp_inputs(["sequences.yml", "taxonomy.yml"])
    fake_bin = MLIB / "tests" / "test_data" / "fake_bin.fna"
    inputs.AddItem(fake_bin, "sequences::metabat2_bin_fasta")
    inputs.Save()
    return inputs


@pytest.fixture
def isolate_input(tmp_inputs):
    inputs = tmp_inputs(["sequences.yml", "taxonomy.yml"])
    fake_iso = MLIB / "tests" / "test_data" / "fake_isolate.fna"
    inputs.AddItem(fake_iso, "sequences::isolate_assembly")
    inputs.Save()
    return inputs


@pytest.fixture
def megahit_input(tmp_inputs):
    inputs = tmp_inputs(["sequences.yml", "taxonomy.yml"])
    fake_asm = MLIB / "tests" / "test_data" / "fake_megahit.fna"
    inputs.AddItem(fake_asm, "sequences::megahit_assembly")
    inputs.Save()
    return inputs


@pytest.fixture
def chromosomal_contig_input(tmp_inputs):
    inputs = tmp_inputs(["sequences.yml", "taxonomy.yml"])
    fake_ctg = MLIB / "tests" / "test_data" / "fake_chrcontig.fna"
    inputs.AddItem(fake_ctg, "sequences::chromosomal_contig")
    inputs.Save()
    return inputs


class TestCheckMOnEachGenomeShape:
    def test_row18_checkm_on_isolate(self, agent, base_resources, all_transforms, isolate_input):
        targets = TargetBuilder()
        targets.Add("taxonomy::checkm_stats")
        task = agent.GenerateWorkflow(
            samples=list(isolate_input.AsSamples("sequences::isolate_assembly")),
            resources=base_resources + [isolate_input],
            transforms=all_transforms,
            targets=targets,
        )
        assert task.ok and len(task.plan.steps) > 0

    def test_row19_checkm_on_metabat2_bin(self, agent, base_resources, all_transforms, bin_only_input):
        targets = TargetBuilder()
        targets.Add("taxonomy::checkm_stats")
        task = agent.GenerateWorkflow(
            samples=list(bin_only_input.AsSamples("sequences::metabat2_bin_fasta")),
            resources=base_resources + [bin_only_input],
            transforms=all_transforms,
            targets=targets,
        )
        assert task.ok and len(task.plan.steps) > 0

    def test_row20_checkm_on_chromosomal_contig(self, agent, base_resources, all_transforms, chromosomal_contig_input):
        targets = TargetBuilder()
        targets.Add("taxonomy::checkm_stats")
        task = agent.GenerateWorkflow(
            samples=list(chromosomal_contig_input.AsSamples("sequences::chromosomal_contig")),
            resources=base_resources + [chromosomal_contig_input],
            transforms=all_transforms,
            targets=targets,
        )
        assert task.ok and len(task.plan.steps) > 0

    def test_row21_checkm_refuses_raw_metagenome(self, agent, base_resources, all_transforms, megahit_input):
        targets = TargetBuilder()
        targets.Add("taxonomy::checkm_stats")
        task = agent.GenerateWorkflow(
            samples=list(megahit_input.AsSamples("sequences::megahit_assembly")),
            resources=base_resources + [megahit_input],
            transforms=all_transforms,
            targets=targets,
        )
        assert (not task.ok) or len(task.plan.steps) == 0, (
            "checkm should refuse a raw megahit_assembly (not a putative_genome)"
        )


class TestCheckMLineageRouting:
    def test_row32_select_chromosomal_contigs_plans(self, agent, base_resources, all_transforms, meta_assembly_with_bam):
        targets = TargetBuilder()
        targets.Add("sequences::chromosomal_contig")
        task = agent.GenerateWorkflow(
            samples=list(meta_assembly_with_bam.AsSamples("sequences::hifiasm_meta_assembly")),
            resources=base_resources + [meta_assembly_with_bam],
            transforms=all_transforms,
            targets=targets,
        )
        assert task.ok and len(task.plan.steps) > 0, (
            "select_chromosomal_contigs should plan against a hifiasm_meta_assembly input"
        )

    def test_row33_checkm_via_chromosomal_contig(self, agent, base_resources, all_transforms, meta_assembly_with_bam):
        targets = TargetBuilder()
        parent = targets.Add("sequences::chromosomal_contig")
        targets.Add("taxonomy::checkm_stats", parents={parent})
        task = agent.GenerateWorkflow(
            samples=list(meta_assembly_with_bam.AsSamples("sequences::hifiasm_meta_assembly")),
            resources=base_resources + [meta_assembly_with_bam],
            transforms=all_transforms,
            targets=targets,
        )
        assert task.ok and len(task.plan.steps) > 0

    def test_row34_checkm_via_metabat2(self, agent, base_resources, all_transforms, meta_assembly_with_bam):
        targets = TargetBuilder()
        parent = targets.Add("sequences::metabat2_bin_fasta")
        targets.Add("taxonomy::checkm_stats", parents={parent})
        task = agent.GenerateWorkflow(
            samples=list(meta_assembly_with_bam.AsSamples("sequences::hifiasm_meta_assembly")),
            resources=base_resources + [meta_assembly_with_bam],
            transforms=all_transforms,
            targets=targets,
        )
        assert task.ok and len(task.plan.steps) > 0

    def test_row35_checkm_via_semibin2(self, agent, base_resources, all_transforms, meta_assembly_with_bam):
        targets = TargetBuilder()
        parent = targets.Add("sequences::semibin2_bin_fasta")
        targets.Add("taxonomy::checkm_stats", parents={parent})
        task = agent.GenerateWorkflow(
            samples=list(meta_assembly_with_bam.AsSamples("sequences::hifiasm_meta_assembly")),
            resources=base_resources + [meta_assembly_with_bam],
            transforms=all_transforms,
            targets=targets,
        )
        assert task.ok and len(task.plan.steps) > 0

    def test_row36_checkm_via_comebin(self, agent, base_resources, all_transforms, meta_assembly_with_bam):
        targets = TargetBuilder()
        parent = targets.Add("sequences::comebin_bin_fasta")
        targets.Add("taxonomy::checkm_stats", parents={parent})
        task = agent.GenerateWorkflow(
            samples=list(meta_assembly_with_bam.AsSamples("sequences::hifiasm_meta_assembly")),
            resources=base_resources + [meta_assembly_with_bam],
            transforms=all_transforms,
            targets=targets,
        )
        assert task.ok and len(task.plan.steps) > 0


class TestCheckMParallelFork:
    def test_row37_target_builder_refuses_duplicate(self):
        targets = TargetBuilder()
        metabat2 = targets.Add("sequences::metabat2_bin_fasta")
        semibin2 = targets.Add("sequences::semibin2_bin_fasta")
        targets.Add("taxonomy::checkm_stats", parents={metabat2})
        targets.Add("taxonomy::checkm_stats", parents={semibin2})
        with pytest.raises(AssertionError, match="already added"):
            targets.Add("taxonomy::checkm_stats", parents={metabat2})

    def test_row37_a_type_name_is_not_a_handle(self):
        targets = TargetBuilder()
        targets.Add("sequences::semibin2_bin_fasta")
        with pytest.raises(AssertionError, match="rather than a handle"):
            targets.Add(
                "taxonomy::checkm_stats",
                parents={"sequences::semibin2_bin_fasta"},
            )
