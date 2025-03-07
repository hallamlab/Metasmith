from pathlib import Path
import shutil
import yaml
import os

from ..coms.ipc import RemoteShell, LiveShell
from ..models.libraries import DataInstanceLibrary, TransformInstanceLibrary
from ..models.remote import GlobusSource, Source, SourceType, Logistics
from ..models.workflow import WorkflowTask
from ..logging import Log
from .presets import Agent

WORK_ROOT = Path("/ws")
HOME_ROOT = Path("/msm_home")
INTERNALS_RELATIVE = Path("_metasmith")

def StageWorkflow(task_dir: Path, force=False):
    assert task_dir.exists(), f"task dir not found [{task_dir}]"
    task = WorkflowTask.Load(task_dir)
    Log.Info(f"staging workflow [{task.plan._key}] with [{len(task.plan.given)}] given data instances")

    work_relative = Path("runs")/task.plan._key
    work_dir = WORK_ROOT/work_relative
    if work_dir.exists():
        Log.Info(f"already staged at [{work_dir}]")
        if force:
            Log.Info(f"removing previous")
            shutil.rmtree(work_dir)
        else:
            return
    work_internals = work_dir/INTERNALS_RELATIVE
    data_dir = WORK_ROOT/"data"
    data_dir.mkdir(parents=True, exist_ok=True)
    work_internals.mkdir(parents=True, exist_ok=True)
    with RemoteShell(HOME_ROOT/"relay/connections/main.in") as extern_shell:
        extern_shell.RegisterOnOut(lambda data: Log.Info(f"ex| {data}"))
        extern_shell.RegisterOnErr(lambda data: Log.Error(f"ex|  {data}"))
        res = extern_shell.Exec(
            f"""
            realpath {task.agent.home.GetPath()}
            """,
            history=True
        )
        extern_root, = [Path(x) for x in res.out]
        extern_work = extern_root/work_relative

    Log.Info(f"work [{work_dir}]")
    Log.Info(f"data [{data_dir}]")
    extern_data = extern_root/data_dir.name
    Log.Info(f"external work [{extern_work}]")
    Log.Info(f"external data [{extern_data}]")

    staged_task_path = work_internals/"task"
    task.SaveAs(Source.FromLocal(staged_task_path))
    staged_task = WorkflowTask.Load(staged_task_path)
    staged_task.plan.PrepareNextflow(
        work_dir=work_dir,
        external_work=extern_work,
    )
    
    params = staged_task.config
    params_yaml = yaml.dump(params)
    Log.Info(f"additional params:")
    for l in params_yaml.split("\n"):
        Log.Info(f"    {l}")

    # nextflow
    nextflow_config_dir = Path("/msm_home/lib/nextflow_config")
    nextflow_parameters = staged_task.config.get("nextflow", {})
    preset_path = nextflow_config_dir/f"{nextflow_parameters.get('preset', 'default')}.nf"
    if not preset_path.exists():
        Log.Error(f"nextflow preset not found [{preset_path}], using default")
        preset_path = nextflow_config_dir/"default.nf"
    with open(preset_path) as f:
        config_raw = "".join(f.readlines())
    nextflow_params = task.config.get("nextflow", {})
    for k, v in nextflow_params.items():
        config_raw = config_raw.replace(f"<{k}>", v)
    with open(work_dir/"workflow.config.nf", "w") as f:
        f.write(config_raw)

    # bootstrap
    bootstrap_path = HOME_ROOT/"lib/msm_bootstrap"
    shutil.copy(bootstrap_path, work_internals)

    Log.Info(f"[{task.plan._key}] staged to [{work_dir}]")
    with LiveShell() as shell:
        res = shell.Exec(f"cd {work_dir} && find .")
        for x in res.out:
            Log.Info(f"    {x}")

def ExecuteWorkflow(key: str):
    workspace = (WORK_ROOT/"runs")/key
    assert workspace.exists(), f"plan folder not found [{workspace}]"
    task = WorkflowTask.Load(workspace/INTERNALS_RELATIVE/f"task")
    Log.Info(f"executing workflow [{task.plan._key}] with [{len(task.plan.steps)}] steps")

    agent = Agent.Load(HOME_ROOT/"lib/agent.yml")
    extern_home = agent.home.GetPath()
    extern_nxf_exe = extern_home/"lib/nextflow"

    Log.Info(f"connecting to relay for external shell")
    with RemoteShell(HOME_ROOT/"relay/connections/main.in") as extern_shell:
        extern_shell.RegisterOnOut(Log.Info)
        extern_shell.RegisterOnErr(Log.Error)
        Log.Info(f"calling nextflow via relay")
        extern_shell.Exec(
            f"""
            cd {extern_home}/runs/{key}
            export NXF_HOME=./.nextflow
            {extern_nxf_exe} -c ./workflow.config.nf \
                -log ./nxf_logs/log \
                run ./workflow.nf \
                -resume \
                -work-dir ./nxf_work
            """,
        )
