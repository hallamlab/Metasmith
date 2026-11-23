import json
from pathlib import Path


from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.remote import Source
from metasmith.models.workflow import NextflowGenContext, WorkflowTask


class TestWorkflowStaging:
    def test_stage_creates_workspace(self, simple_workflow_task, tmp_path):
        task = simple_workflow_task
        work_dir = tmp_path / "stage_ws"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            runtime=Runtime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)

        assert (work_dir / AgentPaths.NXF_WORKFLOW).exists(), "workflow.nf should exist"
        assert (work_dir / AgentPaths.NXF_RES).exists(), "resources file should exist"

    def test_stage_generates_valid_nextflow(self, simple_workflow_task, tmp_path):
        task = simple_workflow_task
        work_dir = tmp_path / "stage_nxf"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            runtime=Runtime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)

        nxf_content = (work_dir / AgentPaths.NXF_WORKFLOW).read_text()
        assert "Orchestrator" in nxf_content
        assert "workflow" in nxf_content
        assert "process" in nxf_content
        assert "o.post" in nxf_content or "o.postIn" in nxf_content

    def test_stage_creates_input_csvs(self, simple_workflow_task, tmp_path):
        task = simple_workflow_task
        work_dir = tmp_path / "stage_csv"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            runtime=Runtime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)

        inputs_dir = work_dir / "inputs"
        assert inputs_dir.exists(), "inputs/ directory should exist"

        csv_files = list(inputs_dir.iterdir())
        assert len(csv_files) > 0, "Should have at least one input CSV"

        for csv_file in csv_files:
            content = csv_file.read_text().strip()
            assert len(content) > 0, f"{csv_file.name} should not be empty"

        ids_dir = work_dir / "input_ids"
        assert not ids_dir.exists(), "input_ids/ sidecar should no longer be written"

    def test_stage_lineage_json(self, simple_workflow_task, tmp_path):
        task = simple_workflow_task
        work_dir = tmp_path / "stage_lin"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            runtime=Runtime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)

        lineage_file = work_dir / "workflow.lineage_of_given.json"
        assert lineage_file.exists(), "lineage file should exist"

        with open(lineage_file) as f:
            lineage = json.load(f)
        assert isinstance(lineage, dict)

    def test_stage_on_exist_error(self, simple_workflow_task, tmp_path):
        task = simple_workflow_task
        work_dir = tmp_path / "stage_exist"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            runtime=Runtime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)
        assert (work_dir / AgentPaths.NXF_WORKFLOW).exists()

        task.PrepareNextflow(context)
        assert (work_dir / AgentPaths.NXF_WORKFLOW).exists()


def test_task_save_load_roundtrip(simple_workflow_task, tmp_path):
    task = simple_workflow_task
    save_dir = tmp_path / "task_save"
    save_dir.mkdir(parents=True)

    task.SaveAs(Source.FromLocal(save_dir))

    loaded = WorkflowTask.Load(save_dir)
    assert loaded.ok == task.ok
    assert len(loaded.plan.steps) == len(task.plan.steps)
    assert len(loaded.plan.given) == len(task.plan.given)
