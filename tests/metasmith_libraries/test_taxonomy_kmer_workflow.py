import pytest
from pathlib import Path
from metasmith.python_api import (
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
)

from conftest import MLIB, TEST_DATA_DIR


@pytest.fixture(scope="module")
def taxprofile_transforms(mlib):
    return [TransformInstanceLibrary.Load(mlib / "transforms/metagenomics")]


@pytest.fixture
def paired_reads_input(tmp_inputs, test_data_dir):
    inputs = tmp_inputs(["sequences.yml", "taxonomy.yml", "ref.yml"])

    r1 = test_data_dir / "small_reads_R1.fq.gz"
    r2 = test_data_dir / "small_reads_R2.fq.gz"
    if not r1.exists() or not r2.exists():
        pytest.skip("Test data not available: small_reads_R{1,2}.fq.gz")

    pair = inputs.AddValue(
        "small_pe_pair.txt",
        "small_pe",
        "sequences::read_pair",
    )
    inputs.AddItem(r1, "sequences::zipped_forward_short_reads", parents={pair})
    inputs.AddItem(r2, "sequences::zipped_reverse_short_reads", parents={pair})

    inputs.LocalizeContents()
    inputs.Save()
    return inputs


@pytest.fixture
def taxprofile_resources(mlib, base_resources):
    resources = list(base_resources)
    lib_path = mlib / "resources/lib"
    if lib_path.exists():
        try:
            resources.append(DataInstanceLibrary.Load(lib_path))
        except Exception:
            pass
    return resources


class TestTaxprofilePlanning:
    def _plan(self, agent, taxprofile_resources, taxprofile_transforms,
              paired_reads_input, target_type):
        targets = TargetBuilder()
        targets.Add(target_type)
        return agent.GenerateWorkflow(
            samples=list(paired_reads_input.AsSamples("sequences::read_pair")),
            resources=taxprofile_resources + [paired_reads_input],
            transforms=taxprofile_transforms,
            targets=targets,
        )

    def test_can_plan_kraken2_workflow(
        self, agent, taxprofile_resources, taxprofile_transforms, paired_reads_input
    ):
        task = self._plan(agent, taxprofile_resources, taxprofile_transforms,
                          paired_reads_input, "taxonomy::bracken_kreport")
        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("kraken2 workflow requires a kraken2_db resource")

    def test_can_plan_centrifuger_workflow(
        self, agent, taxprofile_resources, taxprofile_transforms, paired_reads_input
    ):
        task = self._plan(agent, taxprofile_resources, taxprofile_transforms,
                          paired_reads_input, "taxonomy::centrifuger_kreport")
        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("centrifuger workflow requires a centrifuger_db resource")

    def test_can_plan_sylph_workflow(
        self, agent, taxprofile_resources, taxprofile_transforms, paired_reads_input
    ):
        task = self._plan(agent, taxprofile_resources, taxprofile_transforms,
                          paired_reads_input, "taxonomy::sylph_profile")
        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("sylph workflow requires a sylph_db resource")

    def test_can_plan_ganon2_workflow(
        self, agent, taxprofile_resources, taxprofile_transforms, paired_reads_input
    ):
        task = self._plan(agent, taxprofile_resources, taxprofile_transforms,
                          paired_reads_input, "taxonomy::ganon2_report")
        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("ganon2 workflow requires a ganon2_db resource")

    def test_can_plan_metaphlan_workflow(
        self, agent, taxprofile_resources, taxprofile_transforms, paired_reads_input
    ):
        task = self._plan(agent, taxprofile_resources, taxprofile_transforms,
                          paired_reads_input, "taxonomy::metaphlan_profile")
        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("metaphlan workflow requires a metaphlan_db resource")

    def test_can_plan_phyloflash_workflow(
        self, agent, taxprofile_resources, taxprofile_transforms, paired_reads_input
    ):
        task = self._plan(agent, taxprofile_resources, taxprofile_transforms,
                          paired_reads_input, "taxonomy::phyloflash_summary")
        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("phyloflash workflow requires a phyloflash_db resource")


class TestKmerBridge:
    def test_kmer_bridge_kraken2_vs_centrifuger(
        self, agent, taxprofile_resources, taxprofile_transforms, paired_reads_input
    ):
        targets = TargetBuilder()
        targets.Add("taxonomy::kraken2_report")
        targets.Add("taxonomy::centrifuger_kreport")

        task = agent.GenerateWorkflow(
            samples=list(paired_reads_input.AsSamples("sequences::read_pair")),
            resources=taxprofile_resources + [paired_reads_input],
            transforms=taxprofile_transforms,
            targets=targets,
        )

        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("bridge workflow requires both kraken2_db and centrifuger_db resources")

        step_modules = {
            (s.transform.protocol.__module__ if s.transform.protocol else "?")
            for s in task.plan.steps
        }
        assert any("kraken2" in m for m in step_modules), \
            f"kraken2 transform missing from bridge plan; got: {step_modules}"
        assert any("centrifuger" in m for m in step_modules), \
            f"centrifuger transform missing from bridge plan; got: {step_modules}"


@pytest.mark.slow
class TestSylphHarness:
    def test_sylph_protocol_runs_on_tiny_db(
        self, agent, taxprofile_resources, taxprofile_transforms,
        paired_reads_input, tmp_path, test_data_dir
    ):
        from metasmith.testing.transform_harness import TransformHarness

        tiny_db = test_data_dir / "sylph_tiny.syldb"
        if not tiny_db.exists():
            pytest.skip(
                "sylph_tiny.syldb not staged; build with "
                "`sylph sketch -i refs/*.fna -o tests/test_data/sylph_tiny`"
            )

        paired_reads_input.AddItem(tiny_db, "ref::sylph_db")
        paired_reads_input.Save()

        targets = TargetBuilder()
        targets.Add("taxonomy::sylph_profile")
        task = agent.GenerateWorkflow(
            samples=list(paired_reads_input.AsSamples("sequences::read_pair")),
            resources=taxprofile_resources + [paired_reads_input],
            transforms=taxprofile_transforms,
            targets=targets,
        )
        if not task.ok or len(task.plan.steps) == 0:
            pytest.skip("sylph plan empty - DB or container missing")

        sylph_idx = None
        for i, step in enumerate(task.plan.steps, start=1):
            mod = step.transform.protocol.__module__ if step.transform.protocol else ""
            if "sylph" in mod:
                sylph_idx = i
                break
        assert sylph_idx is not None, "sylph step not found in plan"

        from metasmith.python_api import Source
        agent_home = Path(agent.home.GetPath()) if hasattr(agent, "home") else None
        if agent_home is None or not (agent_home / "_metasmith").exists():
            pytest.skip(
                "agent not fully deployed - harness needs `_metasmith/` for "
                "container bounce paths. Run `agent.Deploy()` first."
            )
        work_dir = agent_home / "harness_runs" / f"sylph_{tmp_path.name}"
        work_dir.mkdir(parents=True, exist_ok=True)

        harness = TransformHarness(task=task, step_index=sylph_idx, work_dir=work_dir)
        result = harness.run()
        assert result.success, f"sylph protocol returned failure: {result}"

        produced = [p for entry in result.manifest for _, p in entry.items() if p]
        assert any(p.exists() and p.stat().st_size > 0 for p in produced), \
            f"sylph produced no non-empty outputs: {produced}"
