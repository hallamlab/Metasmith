"""
End-to-end tests for taxonomy transforms (gtdbtk, metabuli)

These tests verify that taxonomy classification workflows can be:
1. Generated (workflow planning)
2. Staged to the agent
3. Executed via local Docker
4. Produce valid taxonomy TSV outputs
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
    verify_tsv_output,
    MLIB,
    TEST_DATA_DIR,
)


@pytest.fixture(scope="module")
def taxonomy_transforms(mlib):
    """Load metagenomics transforms (includes taxonomy)."""
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/metagenomics"),
    ]


@pytest.fixture
def taxonomy_input(tmp_inputs, test_data_dir):
    """Create input library with an isolate assembly for taxonomy classification.

    After the putative_genome rework, gtdbtk requires `sequences::putative_genome`.
    A raw `sequences::assembly` (mixed-contig metagenome) no longer satisfies it;
    use `sequences::isolate_assembly` for single-organism inputs.
    """
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
    """Create input library typed as a raw (mixed-contig) metagenome assembly.

    Used to verify that gtdbtk REFUSES to run on a raw metagenome assembly,
    which is the intentional behavior change from the putative_genome rework.
    """
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
    """Load taxonomy-specific resources (GTDB database, etc.)."""
    resources = list(base_resources)

    # Add taxonomy database if available
    lib_path = mlib / "resources/lib"
    if lib_path.exists():
        try:
            lib_res = DataInstanceLibrary.Load(lib_path)
            resources.append(lib_res)
        except Exception:
            pass

    return resources


class TestTaxonomyWorkflowGeneration:
    """Tests for workflow generation (planning only)."""

    def test_can_plan_gtdbtk_workflow(
        self, agent, taxonomy_resources, taxonomy_transforms, taxonomy_input
    ):
        """Verify workflow generation for GTDB-Tk classification."""
        targets = TargetBuilder()
        targets.Add("taxonomy::gtdbtk")

        task = agent.GenerateWorkflow(
            samples=list(taxonomy_input.AsSamples("sequences::isolate_assembly")),
            resources=taxonomy_resources + [taxonomy_input],
            transforms=taxonomy_transforms,
            targets=targets,
        )

        # This might fail if GTDB database isn't available
        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("GTDB-Tk workflow requires GTDB database")

    def test_gtdbtk_refuses_raw_metagenome_assembly(
        self, agent, taxonomy_resources, taxonomy_transforms, raw_metagenome_input
    ):
        """Negative test (matrix row #24): a raw `megahit_assembly` is NOT a
        putative_genome, so the gtdbtk workflow must fail to plan.
        """
        targets = TargetBuilder()
        targets.Add("taxonomy::gtdbtk")

        task = agent.GenerateWorkflow(
            samples=list(raw_metagenome_input.AsSamples("sequences::megahit_assembly")),
            resources=taxonomy_resources + [raw_metagenome_input],
            transforms=taxonomy_transforms,
            targets=targets,
        )
        # Either planning fails outright, or the resulting plan is empty:
        assert (not task.ok) or len(task.plan.steps) == 0, (
            "gtdbtk should refuse a raw `megahit_assembly` (no putative_genome lineage)"
        )

    def test_can_plan_metabuli_workflow(
        self, agent, taxonomy_resources, taxonomy_transforms, taxonomy_input
    ):
        """Verify workflow generation for Metabuli classification."""
        targets = TargetBuilder()
        targets.Add("taxonomy::metabuli")

        task = agent.GenerateWorkflow(
            samples=list(taxonomy_input.AsSamples("sequences::isolate_assembly")),
            resources=taxonomy_resources + [taxonomy_input],
            transforms=taxonomy_transforms,
            targets=targets,
        )

        # Skip if required resources aren't available
        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("Metabuli workflow requires database")


@pytest.mark.slow
class TestTaxonomyWorkflowExecution:
    """Full E2E tests that execute workflows via Docker."""

    def test_gtdbtk_e2e(
        self, agent, taxonomy_resources, taxonomy_transforms, taxonomy_input
    ):
        """Full E2E test: stage, run GTDB-Tk, verify taxonomy TSV."""
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
                    # GTDB-Tk produces TSV with specific columns
                    assert verify_tsv_output(full_path), f"Invalid taxonomy TSV: {full_path}"

        assert found_taxonomy, "No GTDB-Tk taxonomy output found"

    def test_metabuli_e2e(
        self, agent, taxonomy_resources, taxonomy_transforms, taxonomy_input
    ):
        """Full E2E test: stage, run Metabuli, verify taxonomy output."""
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
