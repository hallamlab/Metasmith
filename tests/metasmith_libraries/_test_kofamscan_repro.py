import pytest
import time
from pathlib import Path
from metasmith.python_api import (
    Agent,
    Runtime,
    Source,
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Resources,
    Size,
    Duration,
)

from conftest import (
    wait_for_workflow,
    MLIB,
    TEST_DATA_DIR,
    TEST_MSM_HOME,
)


@pytest.fixture(scope="module")
def agent():
    agent_home = Source.FromLocal(TEST_MSM_HOME)
    smith = Agent(
        home=agent_home,
        runtime=Runtime.DOCKER,
    )
    if not (TEST_MSM_HOME / "msm").exists():
        smith.Deploy()
    return smith


@pytest.fixture(scope="module")
def base_resources():
    return [DataInstanceLibrary.Load(MLIB / "resources/env")]


@pytest.fixture(scope="module")
def annotation_transforms():
    return [TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation")]


@pytest.fixture
def orfs_input(tmp_path):
    inputs_dir = tmp_path / "inputs.xgdb"
    inputs = DataInstanceLibrary(inputs_dir)
    inputs.AddTypeLibrary(MLIB / "data_types" / "sequences.yml")
    inputs.AddTypeLibrary(MLIB / "data_types" / "annotation.yml")

    orfs_path = TEST_DATA_DIR / "Ana_PS.ss5.faa"
    assert orfs_path.exists(), f"Test data not found: {orfs_path}"

    inputs.AddItem(orfs_path, "sequences::orfs")
    inputs.LocalizeContents()
    inputs.Save()
    return inputs


@pytest.fixture
def kofam_db_input(tmp_path):
    kofam_dir = TEST_DATA_DIR / "kofam"
    assert (kofam_dir / "profiles").exists(), "KofamScan databases not available"

    inputs_dir = tmp_path / "kofam_db.xgdb"
    inputs = DataInstanceLibrary(inputs_dir)
    inputs.AddTypeLibrary(MLIB / "data_types" / "ref.yml")
    inputs.AddItem(kofam_dir / "profiles", "ref::kofamscan_profiles")
    inputs.AddItem(kofam_dir / "ko_list", "ref::kofamscan_ko_list")
    inputs.LocalizeContents()
    inputs.Save()
    return inputs


def test_kofamscan_repro(agent, base_resources, annotation_transforms, orfs_input, kofam_db_input):
    targets = TargetBuilder()
    targets.Add("annotation::kofamscan_results")

    task = agent.GenerateWorkflow(
        samples=list(orfs_input.AsSamples("sequences::orfs")),
        resources=base_resources + [orfs_input, kofam_db_input],
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
        if "kofamscan_results" in type_name:
            found_results = True
            print(f"  Found result: {path} ({type_name})")

    assert found_results, "No KofamScan results found"
