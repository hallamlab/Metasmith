from __future__ import annotations

import shutil
from pathlib import Path


from metasmith.constants import AgentPaths, MODULE_PATH
from metasmith.env import Runtime
from metasmith.models.remote import Source
from metasmith.models.workflow import NextflowGenContext, WorkflowTask


def stage_task(task: WorkflowTask, rootfs=None) -> tuple[str, Path, WorkflowTask]:
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
        rootfs=rootfs,
    )

    staged.PrepareNextflow(context)
    lib_dir = workspace / "lib"
    lib_dir.mkdir(exist_ok=True)
    shutil.copy(MODULE_PATH / "nextflow_config/Orchestrator.groovy", lib_dir / "Orchestrator.groovy")
    return key, workspace, staged
