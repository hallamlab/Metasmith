from pathlib import Path
import shutil
import yaml

from ..coms.ipc import RemoteShell, LiveShell
from ..models.libraries import DataInstanceLibrary, TransformInstanceLibrary
from ..models.remote import GlobusSource, Source, SourceType, Logistics
from ..models.workflow import WorkflowTask
from ..logging import Log

def StageWorkflow(plan_dir: Path):
    assert plan_dir.exists(), f"plan not found [{plan_dir}]"
    plan_save_path = plan_dir/"plan.json"
    task = WorkflowTask.Load(plan_save_path)
    Log.Info(f"staging workflow [{task.plan._key}] with [{len(task.plan.given)}] given data instances")
    work_root = Path("/ws")
    home_root = Path("/msm_home")
    work_relative = Path("runs")/task.plan._key
    work_dir = work_root/work_relative
    if work_dir.exists(): shutil.rmtree(work_dir)
    internals_relative = Path("metasmith")
    work_internals = work_dir/internals_relative
    data_dir = work_root/"data"
    data_dir.mkdir(parents=True, exist_ok=True)
    work_internals.mkdir(parents=True, exist_ok=True)
    with RemoteShell(home_root/"relay/connections/main.in") as extern_shell:
        extern_shell.RegisterOnOut(lambda data: Log.Info(f"ex| {data}"))
        extern_shell.RegisterOnErr(lambda data: Log.Error(f"ex|  {data}"))
        res = extern_shell.Exec(
            f"""
            realpath {task.agent.workspace}
            realpath {task.agent.home}
            """,
            history=True
        )
        extern_root, extern_home = [Path(x) for x in res.out]
        extern_work = extern_root/work_relative

    Log.Info(f"work [{work_dir}]")
    Log.Info(f"data [{data_dir}]")
    extern_data = extern_root/data_dir.name
    Log.Info(f"external home [{extern_home}]")
    Log.Info(f"external work [{extern_work}]")
    Log.Info(f"external data [{extern_data}]")

    for d in [work_internals, data_dir]:
        d.mkdir(parents=True, exist_ok=True)
    shutil.copy(plan_save_path, work_internals)
    trlib_path = work_internals/"transforms.lib"
    shutil.copytree(plan_dir/trlib_path.name, trlib_path)
    trlib = TransformInstanceLibrary.Load(trlib_path)
    for step in task.plan.steps:
        step.RelinkTransform(trlib)

    # stage data with globus
    given_lib = DataInstanceLibrary(manifest={inst._key: inst for inst in task.plan.given})
    if task.agent.globus_endpoint is None:
        local_source = GlobusSource.FromLocalPath(data_dir).AsSource()
    else:
        local_source = GlobusSource(endpoint=task.agent.globus_endpoint, path=extern_data).AsSource()
    staged_lib: DataInstanceLibrary = given_lib.Move(dest=local_source, label=f"msm_stage:{task.plan._key}")
    staged_sources = {}
    for inst in task.plan.given:
        _staged = GlobusSource.Parse(staged_lib[inst._key].source.address)
        _staged = Source(address=_staged.path, type=SourceType.DIRECT)
        Log.Info(f"staged [{_staged.address}]")
        inst.source = _staged
        staged_sources[inst._key] = _staged
    for step in task.plan.steps:
        for inst in step.uses:
            if inst._key not in staged_sources: continue
            _staged = staged_sources[inst._key]
            inst.source = _staged
    task.plan.PrepareNextflow(
        work_dir=work_dir,
        external_work=extern_work,
        external_home=extern_home,
    )
    
    nextflow_config_dir = Path("/msm_home/lib/nextflow_config")
    with open(nextflow_config_dir/"nxf_slurm.nf") as f:
        config_raw = "".join(f.readlines())
    nextflow_params = task.config.get("nextflow", {})
    for k, v in nextflow_params.items():
        config_raw = config_raw.replace(f"<{k}>", v)
    with open(work_internals/"nxf_config.nf", "w") as f:
        f.write(config_raw)
    Log.Info(f"[{task.plan._key}] staged to [{work_dir}]")

def ExecuteWorkflow(plan_dir: Path):
    assert plan_dir.exists(), f"plan folder not found [{plan_dir}]"
    plan_save_path = plan_dir/"metasmith/plan.json"
    task = WorkflowTask.Load(plan_save_path)
    Log.Info(f"executing workflow [{task.plan._key}] with [{len(task.plan.steps)}] steps")

    with LiveShell() as shell:
        shell.RegisterOnOut(Log.Info)
        shell.RegisterOnErr(Log.Error)

        # params can't be passed into the config (thanks nextflow)
        # they are bing injected during staging via replace
        shell.Exec(
            f"""
            cd {plan_dir}
            nextflow -c ./metasmith/nxf_config.nf \
                -log ./nxf_logs/log \
                run ./metasmith/workflow.nf \
                -resume \
                -work-dir ./nxf_work
            """,
            timeout=None,
        )
