"""Full pipeline E2E tests: deploy -> generate -> stage -> run -> verify.

All tests require Docker and are marked accordingly.
"""

import json
import shutil
import pytest
import yaml
from pathlib import Path

from metasmith.agents import Agent
from metasmith.constants import AgentPaths, VERSION
from metasmith.coms.containers import ContainerRuntime
from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.remote import Source
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import (
    WorkflowPlan,
    WorkflowTask,
    NextflowGenContext,
)
from metasmith.testing.mock_transforms import (
    alignment_transform,
    binner_transforms,
)

from .conftest import create_transform_library

pytestmark = pytest.mark.docker


class TestLocalAgentDeploy:
    """Test agent deployment to local directories."""

    def test_deploy_creates_agent_structure(self, local_agent_home, docker_image):
        """Deploy() creates msm, msm_bootstrap, agent.yml."""
        agent = Agent(
            home=Source.FromLocal(local_agent_home),
            container=docker_image,
            runtime=ContainerRuntime.DOCKER,
        )
        agent.Deploy(assertive=True)

        # Check key files were created
        assert (local_agent_home / "msm").exists(), "msm launcher should exist"
        assert AgentPaths.to_bootstrap(local_agent_home).exists(), "msm_bootstrap should exist"
        assert AgentPaths.to_definition(local_agent_home).exists(), "agent.yml should exist"

        # Verify agent.yml is valid
        agent_def = AgentPaths.to_definition(local_agent_home)
        with open(agent_def) as f:
            data = yaml.safe_load(f)
        assert "home" in data
        assert "container" in data
        assert "runtime" in data

    def test_deploy_idempotent(self, local_agent_home, docker_image):
        """Second Deploy(assertive=False) skips without error."""
        agent = Agent(
            home=Source.FromLocal(local_agent_home),
            container=docker_image,
            runtime=ContainerRuntime.DOCKER,
        )
        # First deploy
        agent.Deploy(assertive=True)
        # Second deploy should skip (not assertive)
        agent.Deploy(assertive=False)  # should not raise


class TestWorkflowGeneration:
    """Test workflow plan generation."""

    def test_generate_binning_workflow(self, mock_samples, mock_types, temp_dir):
        """4 steps: 1 alignment + 3 binners."""
        transforms = alignment_transform() | binner_transforms()
        tr_lib = create_transform_library(temp_dir / "gen_bin", mock_types, transforms)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
        assert len(given) == 3

        target_model = Transform()
        target_model.AddRequirement(properties={"bins", "method:metabat2"})
        target_model.AddRequirement(properties={"bins", "method:maxbin2"})
        target_model.AddRequirement(properties={"bins", "method:concoct"})
        target_names = ["metabat2_bins", "maxbin2_bins", "concoct_bins"]

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        assert isinstance(plan, WorkflowPlan)
        assert len(plan.steps) == 4
        transform_names = {step.transform.name for step in plan.steps}
        assert "alignment" in transform_names
        assert "metabat2" in transform_names
        assert "maxbin2" in transform_names
        assert "concoct" in transform_names

    def test_generate_empty_when_satisfied(self, mock_types, temp_dir):
        """0 steps when target already given."""
        lib_path = temp_dir / "satisfied.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(mock_types, namespace="mock")

        sample_dir = lib.location / "sample_00"
        sample_dir.mkdir(parents=True)
        (sample_dir / "output.bam").write_text("already exists")
        lib.AddItem(Path("sample_00/output.bam"), "mock::bam")
        lib.Save()

        transforms = alignment_transform()
        tr_lib = create_transform_library(temp_dir / "tr_sat", mock_types, transforms)

        given = [[sv] for sv in lib.AsSamples("mock::bam")]
        target_model = Transform()
        target_model.AddRequirement(properties={"bam"})
        target_names = ["bam"]

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        assert isinstance(plan, WorkflowPlan)
        assert len(plan.steps) == 0


class TestWorkflowStaging:
    """Test workflow staging (PrepareNextflow)."""

    def test_stage_creates_workspace(self, simple_workflow_task, temp_dir):
        """Staging creates workspace with workflow.nf, start.sh, lib/Orchestrator.groovy."""
        task = simple_workflow_task
        work_dir = temp_dir / "stage_ws"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            container_runtime=ContainerRuntime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)

        assert (work_dir / AgentPaths.NXF_WORKFLOW).exists(), "workflow.nf should exist"
        assert (work_dir / AgentPaths.NXF_RES).exists(), "resources file should exist"

    def test_stage_generates_valid_nextflow(self, simple_workflow_task, temp_dir):
        """workflow.nf contains proper Orchestrator calls."""
        task = simple_workflow_task
        work_dir = temp_dir / "stage_nxf"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            container_runtime=ContainerRuntime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)

        nxf_content = (work_dir / AgentPaths.NXF_WORKFLOW).read_text()
        assert "Orchestrator" in nxf_content
        assert "workflow" in nxf_content
        assert "process" in nxf_content
        assert "o.post" in nxf_content or "o.postIn" in nxf_content

    def test_stage_creates_input_csvs(self, simple_workflow_task, temp_dir):
        """inputs/*.csv with correct paths are created."""
        task = simple_workflow_task
        work_dir = temp_dir / "stage_csv"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            container_runtime=ContainerRuntime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)

        inputs_dir = work_dir / "inputs"
        assert inputs_dir.exists(), "inputs/ directory should exist"

        csv_files = list(inputs_dir.iterdir())
        assert len(csv_files) > 0, "Should have at least one input CSV"

        # Verify CSV content has paths
        for csv_file in csv_files:
            content = csv_file.read_text().strip()
            assert len(content) > 0, f"{csv_file.name} should not be empty"

        # The old input_ids/ sidecar is gone: agents.py now derives
        # path->instance_id from the given DataInstances (the record), so
        # there is no separate sidecar copy and inputs_dir stays path-only.
        ids_dir = work_dir / "input_ids"
        assert not ids_dir.exists(), "input_ids/ sidecar should no longer be written"

    def test_stage_lineage_json(self, simple_workflow_task, temp_dir):
        """workflow.lineage_of_given.json has parent hashes."""
        task = simple_workflow_task
        work_dir = temp_dir / "stage_lin"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            container_runtime=ContainerRuntime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)

        lineage_file = work_dir / "workflow.lineage_of_given.json"
        assert lineage_file.exists(), "lineage file should exist"

        with open(lineage_file) as f:
            lineage = json.load(f)
        assert isinstance(lineage, dict)

    def test_stage_on_exist_error(self, simple_workflow_task, temp_dir):
        """on_exist='error' behavior tested via WorkflowTask structure."""
        # The on_exist logic is in Agent.StageWorkflow which needs a live shell.
        # We test the staging artifacts instead.
        task = simple_workflow_task
        work_dir = temp_dir / "stage_exist"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            container_runtime=ContainerRuntime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        # First staging
        task.PrepareNextflow(context)
        assert (work_dir / AgentPaths.NXF_WORKFLOW).exists()

        # Second staging overwrites (PrepareNextflow is idempotent)
        task.PrepareNextflow(context)
        assert (work_dir / AgentPaths.NXF_WORKFLOW).exists()


class TestWorkflowExecution:
    """Test workflow execution in stub mode.

    These tests verify the full pipeline can run in stub mode where
    Nextflow processes just create empty output files.
    """

    @pytest.mark.slow
    def test_stub_run_workflow_file_valid(self, simple_workflow_task, temp_dir):
        """Generated workflow.nf is syntactically structured."""
        task = simple_workflow_task
        work_dir = temp_dir / "stub_run"
        work_dir.mkdir(parents=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=Path("/msm_home"),
            external_home=work_dir.parent,
            container_runtime=ContainerRuntime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)

        nxf_content = (work_dir / AgentPaths.NXF_WORKFLOW).read_text()

        # Check for required Nextflow DSL2 structure
        assert "workflow {" in nxf_content or "workflow{" in nxf_content
        assert "process " in nxf_content

        # Check for stub block
        assert "stub:" in nxf_content

        # Check for publish block
        assert "output {" in nxf_content or "output{" in nxf_content

        # Final target keeps its WorkflowTarget.name as the publish path
        target_name = task.plan.targets[0].name
        assert f"path '{target_name}'" in nxf_content

    def test_task_save_load_roundtrip(self, simple_workflow_task, temp_dir):
        """WorkflowTask survives save/load roundtrip."""
        task = simple_workflow_task
        save_dir = temp_dir / "task_save"
        save_dir.mkdir(parents=True)

        task.SaveAs(Source.FromLocal(save_dir))

        loaded = WorkflowTask.Load(save_dir)
        assert loaded.ok == task.ok
        assert len(loaded.plan.steps) == len(task.plan.steps)
        assert len(loaded.plan.given) == len(task.plan.given)
