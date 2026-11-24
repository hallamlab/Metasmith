import pytest
import time
from collections import Counter
from pathlib import Path
from metasmith.python_api import (
    DEFERRED,
    DataInstanceLibrary,
    Plan,
    Spec,
    record_library,
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
    @pytest.mark.parametrize("target", [
        "sequences::metabat2_bin_fasta",
        "sequences::semibin2_bin_fasta",
        "sequences::comebin_bin_fasta",
        "sequences::metadecoder_bin_fasta",
    ])
    def test_can_plan_binner_workflow(
        self, agent, base_resources, binning_transforms, binning_input, target
    ):
        targets = TargetBuilder()
        targets.Add(target)

        task = agent.GenerateWorkflow(
            samples=list(binning_input.AsSamples("sequences::assembly")),
            resources=base_resources + [binning_input],
            transforms=binning_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0, "Workflow should have at least one step"


# das_tool.py and das_tool_metadecoder.py produce the same types. Without the
# binning::lite_binner_set given, quality_bin_fasta once planned the MetaDecoder
# variant, silently replacing COMEBin; with it, the lite plan pins its consumers to
# the MetaDecoder table.
@pytest.mark.parametrize("lite", [False, True])
def test_das_tool_variant_follows_lite_binner_set(tmp_path, lite):
    inputs = DataInstanceLibrary(tmp_path / "inputs.xgdb")
    for name in ("sequences", "ref", "annotation", "taxonomy", "binning", "binning_local", "alignment"):
        inputs.AddTypeLibrary(MLIB / "data_types" / f"{name}.yml")
    asm = inputs.AddItem(DEFERRED, "sequences::assembly")
    inputs.AddItem(DEFERRED, "alignment::bam", parents={asm})
    if lite:
        inputs.AddValue("lite_binner_set.json", {}, "binning::lite_binner_set")
        pin = {"parents": [0]}
        targets = ["binning::metadecoder_contig_to_bin_table"]
    else:
        pin, targets = {}, []
    targets += [{"type": "binning::das_tool_summary", **pin},
                {"type": "binning_local::quality_bin_fasta", **pin}]

    task = Plan(
        [Spec(input_library=record_library(inputs), sample_type=None, target_types=targets)],
        transform_libraries=[MLIB / "transforms" / g for g in ("logistics", "metagenomics")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )

    used = Counter(s.transform.name for s in task.plan.steps)
    assert task.ok, task.plan.dropped_targets
    chosen, excluded = ("das_tool_metadecoder", "comebin") if lite else ("das_tool", "metadecoder")
    assert used[chosen] == 1 and used[excluded] == 0, used


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
