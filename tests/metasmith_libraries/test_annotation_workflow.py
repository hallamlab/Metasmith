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
    verify_json_output,
    verify_tsv_output,
    kofam_db_input,
    interproscan_data_input,
    uniref50_db_input,
    MLIB,
    TEST_DATA_DIR,
)

NIES_102_ORFS = Path("/home/tony/agentic_workspace/main/cyanoverse/tasks/search-transcription-factors/results/prodigal/NIES_102.faa")
NIES_102_ASSEMBLY = Path("/home/tony/agentic_workspace/data/cyanoverse/raw/culture_collections/NIES_102.fasta")
PHASE3_DEEPTFACTOR_REF = Path("/home/tony/agentic_workspace/main/cyanoverse/tasks/search-transcription-factors/results/phase3_tool_evaluation/deeptfactor/prediction_result.txt")
PHASE3_PREDICTF_REF = Path("/home/tony/agentic_workspace/main/cyanoverse/tasks/search-transcription-factors/results/phase3_tool_evaluation/predictf/output/file.out.mapping.TF")

# Cyanoverse assemblies for PILER-CR isolation testing
CYANOVERSE_PILER_CRASH = TEST_DATA_DIR / "DRR287286.fna"  # piler crashes with SIGABRT


@pytest.fixture(scope="module")
def annotation_transforms(mlib):
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/functionalAnnotation"),
    ]


@pytest.fixture
def orfs_input(tmp_inputs, test_data_dir):
    inputs = tmp_inputs(["sequences.yml", "annotation.yml"])

    orfs_path = test_data_dir / "small_orfs.faa"
    if not orfs_path.exists():
        pytest.skip("Test data not available: small_orfs.faa")

    inputs.AddItem(orfs_path, "sequences::orfs")
    inputs.LocalizeContents()
    inputs.Save()

    return inputs


@pytest.fixture
def annotation_resources(mlib, base_resources):
    resources = list(base_resources)

    lib_path = mlib / "resources/lib"
    if lib_path.exists():
        try:
            lib_res = DataInstanceLibrary.Load(lib_path)
            resources.append(lib_res)
        except Exception:
            pass

    return resources


class TestAnnotationWorkflowGeneration:
    def test_can_plan_interproscan_workflow(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::interproscan_json")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"
        assert len(task.plan.steps) > 0

    def test_can_plan_kofamscan_workflow(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::kofamscan_results")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"

    def test_can_plan_proteinbert_workflow(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::proteinbert_embeddings")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"

    def test_can_plan_deepec_workflow(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::deepec_predictions")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"

    def test_can_plan_diamond_uniref50_workflow(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::diamond_uniref50_results")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
            targets=targets,
        )

        if not task.ok:
            pytest.skip("DIAMOND UniRef50 workflow requires database")

    def test_can_plan_deeptfactor_workflow(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::deeptfactor_results")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"

    def test_can_plan_predictf_workflow(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::predictf_results")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
            targets=targets,
        )

        if not task.ok:
            pytest.skip("PredicTF workflow requires database")

    def test_can_plan_ptools_annotation_gather_workflow(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::ptools_annotation_table")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
            targets=targets,
        )

        if not task.ok:
            pytest.skip("ptools_annotation_gather requires UniRef50/KofamScan DBs to be resolvable")

        type_names = [step.transform_instance.transform_name for step in task.plan.steps]
        assert any("deepec" in n for n in type_names), "DeepEC step missing"
        assert any("kofamscan" in n for n in type_names), "KofamScan step missing"
        assert any("diamond_uniref50" in n for n in type_names), "DIAMOND UniRef50 step missing"
        assert any("ptools_annotation_gather" in n for n in type_names), "Gather step missing"

    def test_can_plan_pathologic_workflow(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::pgdb_archive")
        targets.Add("annotation::pgdb_csv_tables")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
            targets=targets,
        )

        if not task.ok:
            pytest.skip("pathologic chain requires UniRef50/KofamScan DBs to be resolvable")

        type_names = [step.transform_instance.transform_name for step in task.plan.steps]
        assert any("pathologic" in n for n in type_names), "Pathologic step missing"

    def test_can_plan_bakta_noncoding_workflow(
        self, agent, annotation_resources, annotation_transforms, orfs_input, tmp_inputs, test_data_dir
    ):
        assembly_inputs = tmp_inputs(["sequences.yml", "annotation.yml"])

        assembly_path = test_data_dir / "small_assembly.fna"
        if not assembly_path.exists():
            pytest.skip("Test data not available: small_assembly.fna")

        assembly_inputs.AddItem(assembly_path, "sequences::assembly")
        assembly_inputs.LocalizeContents()
        assembly_inputs.Save()

        targets = TargetBuilder()
        targets.Add("annotation::bakta_gff")

        task = agent.GenerateWorkflow(
            samples=list(assembly_inputs.AsSamples("sequences::assembly")),
            resources=annotation_resources + [assembly_inputs],
            transforms=annotation_transforms,
            targets=targets,
        )

        if not task.ok:
            pytest.skip("Bakta workflow requires database")


@pytest.mark.slow
class TestAnnotationWorkflowExecution:
    def test_interproscan_e2e(
        self, agent, annotation_resources, annotation_transforms, orfs_input, interproscan_data_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::interproscan_json")
        targets.Add("annotation::interproscan_gff")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input, interproscan_data_input],
            transforms=annotation_transforms,
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

        found_json = False
        found_gff = False

        for path, type_name, endpoint in results.Iterate():
            if "interproscan_json" in type_name:
                found_json = True
                if not path.is_absolute():
                    full_path = results_path / path
                    assert verify_json_output(full_path), f"Invalid JSON: {full_path}"

            if "interproscan_gff" in type_name:
                found_gff = True

        assert found_json, "No InterProScan JSON output found"
        assert found_gff, "No InterProScan GFF output found"

    def test_kofamscan_e2e(
        self, agent, annotation_resources, annotation_transforms, orfs_input, kofam_db_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::kofamscan_results")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input, kofam_db_input],
            transforms=annotation_transforms,
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

        found_results = False
        for path, type_name, endpoint in results.Iterate():
            if "kofamscan_results" in type_name:
                found_results = True

        assert found_results, "No KofamScan results found"

    def test_deepec_e2e(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::deepec_predictions")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
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

        found_predictions = False
        for path, type_name, endpoint in results.Iterate():
            if "deepec_predictions" in type_name:
                found_predictions = True

        assert found_predictions, "No DeepEC predictions found"

    def test_proteinbert_e2e(
        self, agent, annotation_resources, annotation_transforms, orfs_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::proteinbert_embeddings")
        targets.Add("annotation::proteinbert_index")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input],
            transforms=annotation_transforms,
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

        results = wait_for_workflow(agent, task, timeout=900)
        results_path = agent.GetResultSource(task).GetPath()

        found_embeddings = False
        found_index = False

        for path, type_name, endpoint in results.Iterate():
            if "proteinbert_embeddings" in type_name:
                found_embeddings = True
            if "proteinbert_index" in type_name:
                found_index = True

        assert found_embeddings, "No ProteinBERT embeddings found"
        assert found_index, "No ProteinBERT index found"

    def test_diamond_uniref50_e2e(
        self, agent, annotation_resources, annotation_transforms, orfs_input, uniref50_db_input
    ):
        targets = TargetBuilder()
        targets.Add("annotation::diamond_uniref50_results")

        task = agent.GenerateWorkflow(
            samples=list(orfs_input.AsSamples("sequences::orfs")),
            resources=annotation_resources + [orfs_input, uniref50_db_input],
            transforms=annotation_transforms,
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

        found_results = False
        for path, type_name, endpoint in results.Iterate():
            if "diamond_uniref50_results" in type_name:
                found_results = True

        assert found_results, "No DIAMOND UniRef50 results found"

    def test_deeptfactor_e2e(
        self, agent, annotation_resources, annotation_transforms, tmp_inputs
    ):
        if not NIES_102_ORFS.exists():
            pytest.skip("NIES_102 ORFs not available")

        inputs = tmp_inputs(["sequences.yml", "annotation.yml"])
        inputs.AddItem(NIES_102_ORFS, "sequences::orfs")
        inputs.LocalizeContents()
        inputs.Save()

        targets = TargetBuilder()
        targets.Add("annotation::deeptfactor_results")

        task = agent.GenerateWorkflow(
            samples=list(inputs.AsSamples("sequences::orfs")),
            resources=annotation_resources + [inputs],
            transforms=annotation_transforms,
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
                "*": Resources(cpus=4, memory=Size.GB(4)),
            },
        )

        results = wait_for_workflow(agent, task, timeout=1800)
        results_path = agent.GetResultSource(task).GetPath()

        found_results = False
        for path, type_name, endpoint in results.Iterate():
            if "deeptfactor_results" in type_name:
                found_results = True
                full_path = path if path.is_absolute() else results_path / path
                assert full_path.exists(), f"Output file missing: {full_path}"

                content = full_path.read_text()
                lines = content.strip().split("\n")

                header = lines[0].split("\t")
                assert len(header) == 3, f"Expected 3 columns, got {len(header)}: {header}"

                assert abs(len(lines) - 5678) < 100, f"Expected ~5678 lines, got {len(lines)}"

                tf_count = sum(1 for l in lines[1:] if l.split("\t")[1] == "True")
                assert 200 < tf_count < 500, f"Expected ~309 TFs, got {tf_count}"

        assert found_results, "No DeepTFactor results found"

    def test_bakta_noncoding_e2e(
        self, agent, annotation_resources, annotation_transforms, tmp_inputs, bakta_db_input
    ):
        if not NIES_102_ASSEMBLY.exists():
            pytest.skip("NIES_102 assembly not available")

        inputs = tmp_inputs(["sequences.yml", "annotation.yml"])
        inputs.AddItem(NIES_102_ASSEMBLY, "sequences::assembly")
        inputs.LocalizeContents()
        inputs.Save()

        targets = TargetBuilder()
        targets.Add("annotation::bakta_gff")
        targets.Add("annotation::bakta_tsv")

        task = agent.GenerateWorkflow(
            samples=list(inputs.AsSamples("sequences::assembly")),
            resources=annotation_resources + [inputs, bakta_db_input],
            transforms=annotation_transforms,
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

        found_gff = False
        found_tsv = False

        for path, type_name, endpoint in results.Iterate():
            full_path = path if path.is_absolute() else results_path / path
            if "bakta_gff" in type_name:
                found_gff = True
                assert full_path.exists(), f"GFF output missing: {full_path}"
                content = full_path.read_text()
                assert "##gff-version" in content, "Not valid GFF3"
                assert "CDS" not in content, "Bakta non-coding output should not contain CDS"

            if "bakta_tsv" in type_name:
                found_tsv = True
                assert full_path.exists(), f"TSV output missing: {full_path}"

        assert found_gff, "No Bakta GFF output found"
        assert found_tsv, "No Bakta TSV output found"

    def test_bakta_noncoding_piler_success(
        self, agent, annotation_resources, annotation_transforms, tmp_inputs, bakta_db_input
    ):
        if not NIES_102_ASSEMBLY.exists():
            pytest.skip(f"Test assembly not available: {NIES_102_ASSEMBLY}")

        inputs = tmp_inputs(["sequences.yml", "annotation.yml"])
        inputs.AddItem(NIES_102_ASSEMBLY, "sequences::assembly")
        inputs.LocalizeContents()
        inputs.Save()

        targets = TargetBuilder()
        targets.Add("annotation::bakta_gff")
        targets.Add("annotation::bakta_tsv")

        task = agent.GenerateWorkflow(
            samples=list(inputs.AsSamples("sequences::assembly")),
            resources=annotation_resources + [inputs, bakta_db_input],
            transforms=annotation_transforms,
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

        found_gff = False
        for path, type_name, endpoint in results.Iterate():
            full_path = path if path.is_absolute() else results_path / path
            if "bakta_gff" in type_name:
                found_gff = True
                assert full_path.exists(), f"GFF output missing: {full_path}"
                content = full_path.read_text()
                assert "##gff-version" in content, "Not valid GFF3"
                assert "CDS" not in content, "Non-coding output should not contain CDS"
                assert "PILER-CR" in content, "PILER-CR CRISPR entries missing from GFF3"

        assert found_gff, "No Bakta GFF output found"

    def test_bakta_noncoding_piler_crash(
        self, agent, annotation_resources, annotation_transforms, tmp_inputs, bakta_db_input
    ):
        if not CYANOVERSE_PILER_CRASH.exists():
            pytest.skip(f"Test assembly not available: {CYANOVERSE_PILER_CRASH}")

        inputs = tmp_inputs(["sequences.yml", "annotation.yml"])
        inputs.AddItem(CYANOVERSE_PILER_CRASH, "sequences::assembly")
        inputs.LocalizeContents()
        inputs.Save()

        targets = TargetBuilder()
        targets.Add("annotation::bakta_gff")
        targets.Add("annotation::bakta_tsv")

        task = agent.GenerateWorkflow(
            samples=list(inputs.AsSamples("sequences::assembly")),
            resources=annotation_resources + [inputs, bakta_db_input],
            transforms=annotation_transforms,
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

        found_gff = False
        found_tsv = False
        for path, type_name, endpoint in results.Iterate():
            full_path = path if path.is_absolute() else results_path / path
            if "bakta_gff" in type_name:
                found_gff = True
                assert full_path.exists(), f"GFF output missing: {full_path}"
                content = full_path.read_text()
                assert "##gff-version" in content, "Not valid GFF3"
                assert "CDS" not in content, "Non-coding output should not contain CDS"
                # PILER-CR crashes on this sample — GFF3 should still be valid bakta output
                # CRISPR entries are not expected (piler crash = no CRISPR data)

            if "bakta_tsv" in type_name:
                found_tsv = True
                assert full_path.exists(), f"TSV output missing: {full_path}"

        assert found_gff, "No Bakta GFF output found"
        assert found_tsv, "No Bakta TSV output found"

    def test_predictf_e2e(
        self, agent, annotation_resources, annotation_transforms, tmp_inputs, predictf_db_input
    ):
        if not NIES_102_ORFS.exists():
            pytest.skip("NIES_102 ORFs not available")

        inputs = tmp_inputs(["sequences.yml", "annotation.yml"])
        inputs.AddItem(NIES_102_ORFS, "sequences::orfs")
        inputs.LocalizeContents()
        inputs.Save()

        targets = TargetBuilder()
        targets.Add("annotation::predictf_results")
        targets.Add("annotation::predictf_potential")

        task = agent.GenerateWorkflow(
            samples=list(inputs.AsSamples("sequences::orfs")),
            resources=annotation_resources + [inputs, predictf_db_input],
            transforms=annotation_transforms,
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
                "*": Resources(cpus=4, memory=Size.GB(4)),
            },
        )

        results = wait_for_workflow(agent, task, timeout=1800)
        results_path = agent.GetResultSource(task).GetPath()

        found_tf = False
        found_potential = False

        for path, type_name, endpoint in results.Iterate():
            full_path = path if path.is_absolute() else results_path / path
            if "predictf_results" in type_name:
                found_tf = True
                assert full_path.exists(), f"TF results missing: {full_path}"
                content = full_path.read_text()
                lines = content.strip().split("\n")
                data_lines = [l for l in lines if not l.startswith("#")]
                assert 10 < len(data_lines) < 50, f"Expected ~20 TFs, got {len(data_lines)}"

            if "predictf_potential" in type_name:
                found_potential = True
                assert full_path.exists(), f"Potential TF results missing: {full_path}"

        assert found_tf, "No PredicTF results found"
        assert found_potential, "No PredicTF potential results found"
