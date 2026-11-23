from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from metasmith.constants import AgentPaths, MODULE_PATH
from metasmith.env import Runtime
from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.remote import Source
from metasmith.models.workflow import NextflowGenContext, WorkflowTask
from metasmith.testing.pool_fixtures import pool_backed


@pytest.fixture
def mock_samples(tmp_path, mock_types) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(mock_types, namespace="mock")

    for i in range(3):
        sid = f"sample_{i:02d}"
        sdir = lib.location / sid
        sdir.mkdir(parents=True, exist_ok=True)
        (sdir / "metadata.json").write_text(f'{{"id": "{sid}"}}', encoding="utf-8")
        (sdir / "reads.fq").write_text(f">read_{i}\nACGT\n", encoding="utf-8")
        (sdir / "assembly.fa").write_text(f">contig_{i}\nACGTACGT\n", encoding="utf-8")

        m = lib.AddItem(Path(f"{sid}/metadata.json"), "mock::sample_metadata")
        r = lib.AddItem(Path(f"{sid}/reads.fq"), "mock::reads", parents=[m])
        lib.AddItem(Path(f"{sid}/assembly.fa"), "mock::assembly", parents=[r])

    pool_backed(lib)
    lib.Save()
    return lib


def stage_task(task: WorkflowTask) -> tuple[str, Path, WorkflowTask]:
    key = task.GetKey()
    task_path = AgentPaths.to_task(key)
    task_path.parent.mkdir(parents=True, exist_ok=True)
    task.SaveAs(Source.FromLocal(task_path))

    staged = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])
    workspace = task_path.parent.parent
    workspace.mkdir(parents=True, exist_ok=True)

    context = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=workspace,
        external_work=workspace,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=AgentPaths.HOME_ROOT,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    )

    staged.PrepareNextflow(context)
    lib_dir = workspace / "lib"
    lib_dir.mkdir(exist_ok=True)
    shutil.copy(MODULE_PATH / "nextflow_config/Orchestrator.groovy", lib_dir / "Orchestrator.groovy")
    return key, workspace, staged
