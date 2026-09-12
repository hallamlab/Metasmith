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
def taxonomy_transforms(mlib):
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/metagenomics"),
    ]


@pytest.fixture
def taxonomy_input(tmp_inputs, test_data_dir):
    inputs = tmp_inputs(["sequences.yml", "taxonomy.yml"])

    assembly_path = test_data_dir / "small_assembly.fna"
    if not assembly_path.exists():
        pytest.skip("Test data not available: small_assembly.fna")

    inputs.AddItem(assembly_path, "sequences::isolate_assembly")
    inputs.LocalizeContents()
    inputs.Save()

    return inputs


@pytest.fixture
def raw_metagenome_input(tmp_inputs, test_data_dir):
    inputs = tmp_inputs(["sequences.yml", "taxonomy.yml"])

    assembly_path = test_data_dir / "small_assembly.fna"
    if not assembly_path.exists():
        pytest.skip("Test data not available: small_assembly.fna")

    inputs.AddItem(assembly_path, "sequences::megahit_assembly")
    inputs.LocalizeContents()
    inputs.Save()

    return inputs


@pytest.fixture
def taxonomy_resources(mlib, base_resources):
    resources = list(base_resources)

    lib_path = mlib / "resources/lib"
    if lib_path.exists():
        try:
            lib_res = DataInstanceLibrary.Load(lib_path)
            resources.append(lib_res)
        except Exception:
            pass

    return resources


class TestTaxonomyWorkflowGeneration:
    def test_can_plan_gtdbtk_workflow(
        self, agent, taxonomy_resources, taxonomy_transforms, taxonomy_input
    ):
        targets = TargetBuilder()
        targets.Add("taxonomy::gtdbtk")

        task = agent.GenerateWorkflow(
            samples=list(taxonomy_input.AsSamples("sequences::isolate_assembly")),
            resources=taxonomy_resources + [taxonomy_input],
            transforms=taxonomy_transforms,
            targets=targets,
        )

        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("GTDB-Tk workflow requires GTDB database")

    def test_gtdbtk_refuses_raw_metagenome_assembly(
        self, agent, taxonomy_resources, taxonomy_transforms, raw_metagenome_input
    ):
        targets = TargetBuilder()
        targets.Add("taxonomy::gtdbtk")

        task = agent.GenerateWorkflow(
            samples=list(raw_metagenome_input.AsSamples("sequences::megahit_assembly")),
            resources=taxonomy_resources + [raw_metagenome_input],
            transforms=taxonomy_transforms,
            targets=targets,
        )
        assert (not task.ok) or len(task.plan.steps) == 0, (
            "gtdbtk should refuse a raw `megahit_assembly` (no putative_genome lineage)"
        )

    def test_can_plan_metabuli_workflow(
        self, agent, taxonomy_resources, taxonomy_transforms, taxonomy_input
    ):
        targets = TargetBuilder()
        targets.Add("taxonomy::metabuli")

        task = agent.GenerateWorkflow(
            samples=list(taxonomy_input.AsSamples("sequences::isolate_assembly")),
            resources=taxonomy_resources + [taxonomy_input],
            transforms=taxonomy_transforms,
            targets=targets,
        )

        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("Metabuli workflow requires database")


@pytest.mark.slow
class TestTaxonomyWorkflowExecution:
    def test_gtdbtk_e2e(
        self, agent, taxonomy_resources, taxonomy_transforms, taxonomy_input
    ):
        targets = TargetBuilder()
        targets.Add("taxonomy::gtdbtk")

        task = agent.GenerateWorkflow(
            samples=list(taxonomy_input.AsSamples("sequences::isolate_assembly")),
            resources=taxonomy_resources + [taxonomy_input],
            transforms=taxonomy_transforms,
            targets=targets,
        )

        if not task.ok:
            pytest.skip("GTDB-Tk workflow requires GTDB database")

        agent.StageWorkflow(task, on_exist="clear")
        agent.RunWorkflow(
            task,
            config_file=agent.GetNxfConfigPresets()["local"],
            params=dict(
                executor=dict(cpus=4, queueSize=1),
                process=dict(tries=1),
            ),
        )

        results = wait_for_workflow(agent, task, timeout=1200)
        results_path = agent.GetResultSource(task).GetPath()

        found_taxonomy = False
        for path, type_name, endpoint in results.Iterate():
            if "gtdbtk" in type_name and "raw" not in type_name:
                found_taxonomy = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert verify_tsv_output(full_path), f"Invalid taxonomy TSV: {full_path}"

        assert found_taxonomy, "No GTDB-Tk taxonomy output found"

    def test_metabuli_e2e(
        self, agent, taxonomy_resources, taxonomy_transforms, taxonomy_input
    ):
        targets = TargetBuilder()
        targets.Add("taxonomy::metabuli")

        task = agent.GenerateWorkflow(
            samples=list(taxonomy_input.AsSamples("sequences::isolate_assembly")),
            resources=taxonomy_resources + [taxonomy_input],
            transforms=taxonomy_transforms,
            targets=targets,
        )

        if not task.ok:
            pytest.skip("Metabuli workflow requires database")

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

        found_taxonomy = False
        for path, type_name, endpoint in results.Iterate():
            if "metabuli" in type_name:
                found_taxonomy = True

        assert found_taxonomy, "No Metabuli taxonomy output found"
