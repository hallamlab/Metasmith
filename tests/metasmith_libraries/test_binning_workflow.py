import pytest
import time
from pathlib import Path
from metasmith.python_api import (
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Resources,
    Size,
    Duration,
)

from conftest import (
    wait_for_workflow,
    verify_fasta_output,
    verify_tsv_output,
    MLIB,
    TEST_DATA_DIR,
)


@pytest.fixture(scope="module")
def binning_transforms(mlib):
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/metagenomics"),
    ]


@pytest.fixture
def binning_input(tmp_inputs, test_data_dir, ab48_bam):
    inputs = tmp_inputs(["sequences.yml", "alignment.yml", "binning.yml"])

    assembly_path = test_data_dir / "ab48_community" / "ABC-240403_KD.fna"
    bam_path = ab48_bam

    asm = inputs.AddItem(assembly_path, "sequences::assembly")
    inputs.AddItem(bam_path, "alignment::bam", parents={asm})

    inputs.Save()

    return inputs


class TestBinningWorkflowGeneration:
    def test_can_plan_metabat2_workflow(
        self, agent, base_resources, binning_transforms, binning_input
    ):
        targets = TargetBuilder()
        targets.Add("sequences::metabat2_bin_fasta")

        task = agent.GenerateWorkflow(
            samples=list(binning_input.AsSamples("sequences::assembly")),
            resources=base_resources + [binning_input],
            transforms=binning_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0, "Workflow should have at least one step"

    def test_can_plan_semibin2_workflow(
        self, agent, base_resources, binning_transforms, binning_input
    ):
        targets = TargetBuilder()
        targets.Add("sequences::semibin2_bin_fasta")

        task = agent.GenerateWorkflow(
            samples=list(binning_input.AsSamples("sequences::assembly")),
            resources=base_resources + [binning_input],
            transforms=binning_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"

    def test_can_plan_comebin_workflow(
        self, agent, base_resources, binning_transforms, binning_input
    ):
        targets = TargetBuilder()
        targets.Add("sequences::comebin_bin_fasta")

        task = agent.GenerateWorkflow(
            samples=list(binning_input.AsSamples("sequences::assembly")),
            resources=base_resources + [binning_input],
            transforms=binning_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"


@pytest.mark.slow
class TestBinningWorkflowExecution:
    def test_metabat2_e2e(
        self, agent, base_resources, binning_transforms, binning_input
    ):
        targets = TargetBuilder()
        targets.Add("sequences::metabat2_bin_fasta")
        targets.Add("binning::metabat2_contig_to_bin_table")

        task = agent.GenerateWorkflow(
            samples=list(binning_input.AsSamples("sequences::assembly")),
            resources=base_resources + [binning_input],
            transforms=binning_transforms,
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
            resource_overrides={
                "*": Resources(cpus=4, memory=Size.GB(8)),
            },
        )

        results = wait_for_workflow(agent, task, timeout=600)
        results_path = agent.GetResultSource(task).GetPath()

        found_bins = False
        found_table = False

        for path, type_name, endpoint in results.Iterate():
            if "metabat2_bin_fasta" in type_name:
                found_bins = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert verify_fasta_output(full_path), f"Invalid bin FASTA: {full_path}"

            if "metabat2_contig_to_bin_table" in type_name:
                found_table = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert verify_tsv_output(full_path), f"Invalid table: {full_path}"

        assert found_bins, "No MetaBAT2 bin FASTA files produced"
        assert found_table, "No MetaBAT2 contig-to-bin table produced"

    def test_semibin2_e2e(
        self, agent, base_resources, binning_transforms, binning_input
    ):
        targets = TargetBuilder()
        targets.Add("sequences::semibin2_bin_fasta")

        task = agent.GenerateWorkflow(
            samples=list(binning_input.AsSamples("sequences::assembly")),
            resources=base_resources + [binning_input],
            transforms=binning_transforms,
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
            resource_overrides={
                "*": Resources(cpus=4, memory=Size.GB(8)),
            },
        )

        results = wait_for_workflow(agent, task, timeout=600)
        results_path = agent.GetResultSource(task).GetPath()

        found_bins = False
        for path, type_name, endpoint in results.Iterate():
            if "semibin2_bin_fasta" in type_name:
                found_bins = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert verify_fasta_output(full_path), f"Invalid bin: {full_path}"

        assert found_bins, "No SemiBin2 bin FASTA files produced"

    def test_comebin_e2e(
        self, agent, base_resources, binning_transforms, binning_input
    ):
        targets = TargetBuilder()
        targets.Add("sequences::comebin_bin_fasta")
        targets.Add("binning::comebin_contig_to_bin_table")

        task = agent.GenerateWorkflow(
            samples=list(binning_input.AsSamples("sequences::assembly")),
            resources=base_resources + [binning_input],
            transforms=binning_transforms,
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
            resource_overrides={
                "*": Resources(cpus=4, memory=Size.GB(8)),
            },
        )

        results = wait_for_workflow(agent, task, timeout=1800)
        results_path = agent.GetResultSource(task).GetPath()

        found_bins = False
        found_table = False

        for path, type_name, endpoint in results.Iterate():
            if "comebin_bin_fasta" in type_name:
                found_bins = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert verify_fasta_output(full_path), f"Invalid bin FASTA: {full_path}"

            if "comebin_contig_to_bin_table" in type_name:
                found_table = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert verify_tsv_output(full_path), f"Invalid table: {full_path}"

        assert found_bins, "No COMEBin bin FASTA files produced"
        assert found_table, "No COMEBin contig-to-bin table produced"
