from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass, field
import tempfile
import shutil
from typing import Iterable, Literal
import yaml
import time

from .hashing import KeyGenerator
from .coms.ipc import LiveShell, ShellResult, RemoveLeadingIndent
from .logging import Log
from .coms.containers import Container, CONTAINER_RUNTIME
from .coms.ipc import RemoteShell
from .models.remote import Logistics, Source, SourceType, SshSource
from .models.workflow import WorkflowStep, WorkflowPlan, WorkflowTask
from .models.libraries import DataInstanceLibrary, DataInstance, TransformInstanceLibrary, TransformInstance
from .models.solver import Endpoint, Dependency, Transform, _solve_by_bounded_dfs

class AgentPaths:
    WORK_ROOT = Path("/ws")
    HOME_ROOT = Path("/msm_home")
    INTERNALS = Path("_metasmith")
    STAGED = Path("runs")
    TASK = Path("task")

    @classmethod
    def to_task(cls, key: str, root: Path=None):
        if root is None: root = cls.HOME_ROOT
        return root/(cls.STAGED/key)/cls.INTERNALS/cls.TASK
    
    @classmethod
    def to_definition(cls, root: Path=None):
        if root is None: root = cls.HOME_ROOT
        return root/"lib/agent.yml"
    
    @classmethod
    def to_relay_coms(cls, root: Path=None):
        if root is None: root = cls.HOME_ROOT
        return root/"relay/connections/main.in"

class AgentShell:
    def __init__(self, agent: Agent):
        self.agent = agent
        self.shell = LiveShell()
        self.paused_out = False
        self.paused_err = False
        def _on_out(x: str):
            if self.paused_out: return
            Log.Info(f"> {x}\x1b[0;m", timestamp=False) # to escape nextflow colours
        def _on_err(x: str):
            if self.paused_err: return
            Log.Error(f"> {x}", timestamp=False)
        self.shell.RegisterOnOut(_on_out)
        self.shell.RegisterOnErr(_on_err)
        Log.Info(f"connecting to deployed agent")
        self.agent._run_setup(self.shell)
        self.shell.Exec(f"cd {agent.home.GetPath()}")
        Log.Info(f"starting relay service")
        self.shell.Exec(f"./relay/msm_relay start")

    def __enter__(self):
        return self.shell

    def __exit__(self, exc_type, exc_val, exc_tb):
        Log.Info(f"closing connection")
        self.agent._run_cleanup(self.shell)
        if self.agent._is_ssh():
            self.shell.Exec("exit")
        self.shell.__exit__(exc_type, exc_val, exc_tb)

class PausedShell:
    def __init__(self, shell: AgentShell, err=False):
        self.shell = shell
        self.originals = shell.paused_out, shell.paused_err
        self.shell.paused_out = True
        self.shell.paused_err = err
    
    def __enter__(self):
        return self.shell
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        oo, oe = self.originals
        self.shell.paused_out = oo
        self.shell.paused_err = oe

@dataclass
class Agent:
    home: Source
    setup_commands: list[str] = field(default_factory=list)
    container: str = "docker://quay.io/hallamlab/metasmith:latest"

    def _is_ssh(self):
        return self.home.type == SourceType.SSH

    def Pack(self):
        return dict(
            setup_commands=list(self.setup_commands),
            home=self.home.Pack(),
            container=self.container,
        )
    
    def Save(self, file_path: Path):
        with open(file_path, "w") as f:
            yaml.dump(self.Pack(), f)

    @classmethod
    def Unpack(cls, data):
        data["home"] = Source.Unpack(data["home"])
        return cls(**data)
    
    @classmethod
    def Load(cls, file_path: Path):
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
        return cls.Unpack(data)

    def _run_setup(self, shell: LiveShell, timeout: int = None):
        if self._is_ssh():
            shell.Exec(f"ssh {SshSource.Parse(self.home.address).host}")
            SUCCESS = f"ssh_connected_flag.{KeyGenerator.FromInt(2**42)}"
            res = shell.Exec(f'[ ! -z "$SSH_CONNECTION" ] && echo "{SUCCESS}"', timeout=timeout, history=True)
            if not any(SUCCESS in x for x in res.out):
                assert False, f"ssh connection failed {res.err}"

        for cmd in self.setup_commands:
            shell.Exec(cmd, timeout=timeout)

    def _run_cleanup(self, shell: LiveShell):
        pass

    def Deploy(self):
        with LiveShell() as shell, tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            shell.RegisterOnOut(Log.Info)
            shell.RegisterOnErr(Log.Error)
            def do_step(cmd: str, timeout=15):
                str_cmd = RemoveLeadingIndent(cmd)
                for x in str_cmd.split("\n"):
                    Log.Info(f">>> {x}")
                return shell.Exec(cmd, timeout=timeout, history=True)

            def _remote_file(x: str|Path, dest: Path, executable=False):
                if isinstance(x, str):
                    x = RemoveLeadingIndent(x)
                    fpath = tmpdir/f"{dest.name}"
                    with open(fpath, "w") as f:
                        f.write(x)
                    if executable: os.chmod(fpath, 0o755)
                else:
                    fpath = x
                mover = Logistics()
                mover.QueueTransfer(
                    src=Source.FromLocal(fpath),
                    dest=self.home.ReplacePathWith(dest)
                )
                Log.Info(f"deploying file [{dest}]")
                res = mover.ExecuteTransfers()
                if isinstance(x, Path):
                    _err = f"Failed to deploy file [{x}] -> [{dest}]"
                else:
                    _err = f"Failed to deploy file to  [{dest}]"
                assert len(res.completed) == 1, _err

            self._run_setup(shell)
            res = shell.Exec(f"""
                realpath {self.home.GetPath()}
                realpath ~
            """, history=True)
            resolved_msmhome, resolved_home = [Path(x.strip()) for x in res.out]

            dev_src = resolved_msmhome/"dev/metasmith"
            def make_dev_container(c: Container):
                return Container(
                    image=c.image,
                    binds=c.binds+[
                        (dev_src, Path("/opt/conda/envs/metasmith_env/lib/python3.12/site-packages/metasmith")),
                    ],
                    workdir=c.workdir,
                    runtime=c.runtime,
                )
            container = Container(
                image=self.container,
                container_cache=resolved_msmhome, # just so the main container is saved here
                binds=[
                    (resolved_msmhome, Path("/msm_home")),
                    (Path(resolved_home)/".globus", Path(resolved_home)/".globus"),
                    (Path(resolved_home)/".globusonline", Path(resolved_home)/".globusonline"),
                ],
                runtime=CONTAINER_RUNTIME.APPTAINER
            )
            container_dev = make_dev_container(container)
            _cmds = [f"mkdir -p {p}" for p, _ in container.binds]
            do_step("\n".join(_cmds))
            do_step(f"[ -e {container._get_local_path()} ] || {container.MakePullCommand()}", timeout=None)

            _remote_file(
                f"""
                #!/bin/bash
                if [ -e "{dev_src}" ]; then
                    echo "including dev binds"
                    {container_dev.MakeRunCommand(local=f"{resolved_msmhome}/metasmith.sif")} $@
                else
                    {container.MakeRunCommand(local=f"{resolved_msmhome}/metasmith.sif")} $@
                fi
                """,
                dest=resolved_msmhome/"msm_stub",
                executable=True,
            )

            HERE='$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )'
            _remote_file(
                f"""
                #!/bin/bash
                HERE={HERE}
                $HERE/msm_stub metasmith $@
                """,
                dest=resolved_msmhome/"msm",
                executable=True,
            )

            do_step(f"cd {resolved_msmhome} && ./msm api deploy_from_container")

            _remote_copy = Agent(
                setup_commands=self.setup_commands,
                home=Source.FromLocal(resolved_msmhome),
                container=self.container,
            )
            _remote_file(
                yaml.dump(_remote_copy.Pack()),
                dest=AgentPaths.to_definition(resolved_msmhome),
            )

            bootstrap_container = Container(
                image=self.container,
                binds=[
                    (Path("./.msm"), Path("/msm_home")),
                    (Path("./"), Path("/ws")),
                    (resolved_msmhome, Path("/agent_home")),
                ],
                workdir=Path("/ws"),
                runtime=CONTAINER_RUNTIME.APPTAINER,
            )
            bootstrap_container_dev = make_dev_container(bootstrap_container)
            _remote_file(
                f"""
                #!/bin/bash
                function run_container {{
                    if [ -e "{dev_src}" ]; then
                        echo "including dev binds"
                        {bootstrap_container_dev.MakeRunCommand(local="./metasmith.sif")} $@
                    else
                        {bootstrap_container.MakeRunCommand(local="./metasmith.sif")} $@
                    fi
                }}

                echo "get container =================="
                cp {resolved_msmhome}/metasmith.sif ./
                mkdir -p ./.msm
                echo "deploy ========================="
                run_container metasmith api deploy_from_container -a workspace=./.msm
                echo "post deploy ===================="
                find .
                ls -lh .
                echo "relay =========================="
                ./.msm/relay/msm_relay start
                echo "execute ========================"
                run_container metasmith api execute_transform -a step_index=$1
                echo "post execute ==================="
                find .
                ls -lh .
                echo "exit ==========================="
                ./.msm/relay/msm_relay stop
                sleep 1
                """,
                dest=resolved_msmhome/"lib/msm_bootstrap",
                executable=True,
            )

            HERE = Path(__file__).parent
            _remote_file(HERE/"nextflow_config", resolved_msmhome/"lib/nextflow_config")
            self._run_cleanup(shell)

    def GenerateWorkflow(self, given: Iterable[DataInstanceLibrary], transforms: Iterable[TransformInstanceLibrary], targets: Iterable[Endpoint], config: dict=None):
        plan = WorkflowPlan.Generate(given, transforms, targets)
        if config is None: config = {}
        task = WorkflowTask(plan=plan, data_libraries=given,transform_libraries=transforms, config=config)
        return task

    def StageWorkflow(self, task: WorkflowTask, on_exist: str = "skip", view: bool=False):
        assert on_exist in {"skip", "error", "clear", "overwrite"}
        agent_shell = AgentShell(self)
        with agent_shell as sh_remote:
            remote_path = AgentPaths.to_task(task.plan._key, root=self.home.GetPath())
            remote_work_path = remote_path.parent.parent
            with PausedShell(agent_shell):
                FLAG = "task already staged"
                res = sh_remote.Exec(f'[ -e {remote_work_path} ] && echo "{FLAG}"', history=True)
                if FLAG in res.out:
                    _msg = f"task already staged at [{remote_work_path}]"
                    if on_exist == "error":
                        raise FileExistsError(_msg)
                    Log.Warn(_msg)
                    if on_exist == "skip":
                        return
                    if on_exist == "clear":
                        Log.Warn(f"clearing previous")
                        _to_delete_src = remote_work_path
                        _to_delete = _to_delete_src.with_suffix(".to_delete")
                        sh_remote.Exec(f"mv {_to_delete_src} {_to_delete} && rm -rf {_to_delete}")
                    elif on_exist == "overwrite":
                        Log.Warn(f"overwriting previous")

            Log.Info(f"sending metadata for workflow [{task.plan._key}]")
            task.SaveAs(self.home.ReplacePathWith(remote_path))
            Log.Info(f"staging")
            sh_remote.Exec(f"./msm api stage_workflow -a task_key={task.plan._key} -a view={view}")

    def RunWorkflow(self, task: WorkflowTask|str):
        if isinstance(task, WorkflowTask):
            key = task.plan._key
        else:
            key = str(task)
        with AgentShell(self) as sh_remote:
            Log.Info(f"executing workflow")
            sh_remote.Exec(f"./msm api execute_workflow -a key={key}")

# ===========================================================================
# calls to staged Agent

def StageWorkflow(task_key: str, view=False):
    agent = Agent.Load(AgentPaths.HOME_ROOT/"lib/agent.yml")
    task_dir = agent.home.GetPath()/AgentPaths.to_task(task_key)
    assert task_dir.exists(), f"task dir not found [{task_dir}]"
    task = WorkflowTask.Load(task_dir)
    Log.Info(f"staging workflow [{task.plan._key}] with [{len(task.plan.given)}] given data instances")

    work_relative = AgentPaths.STAGED/task.plan._key
    work_dir = AgentPaths.WORK_ROOT/work_relative
    work_internals = work_dir/AgentPaths.INTERNALS
    data_dir = AgentPaths.WORK_ROOT/"data"
    data_dir.mkdir(parents=True, exist_ok=True)
    work_internals.mkdir(parents=True, exist_ok=True)
    with RemoteShell(AgentPaths.to_relay_coms()) as extern_shell:
        extern_shell.RegisterOnOut(lambda data: Log.Info(f"ex| {data}"))
        extern_shell.RegisterOnErr(lambda data: Log.Error(f"ex|  {data}"))
        res = extern_shell.Exec(
            f"""
            realpath {agent.home.GetPath()}
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
    params = task.config
    params_yaml = yaml.dump(params)
    Log.Info(f"additional params:")
    lines = params_yaml.split("\n")
    if lines[-1] == "": lines = lines[:-1]
    for l in lines:
        Log.Info(f"    {l}")

    # nextflow
    Log.Info(f"compiling nextflow script")
    task.plan.PrepareNextflow(
        work_dir=work_dir,
        external_work=extern_work,
    )
    nextflow_config_dir = Path("/msm_home/lib/nextflow_config")
    nextflow_parameters = task.config.get("nextflow", {})
    preset_path = nextflow_config_dir/f"{nextflow_parameters.get('preset', 'default')}.nf"
    if not preset_path.exists():
        Log.Warn(f"nextflow preset not found [{preset_path}], using default")
        preset_path = nextflow_config_dir/"default.nf"
    else:
        Log.Info(f"using preset [{preset_path.stem}]")
    with open(preset_path) as f:
        config_raw = "".join(f.readlines())
    nextflow_params = task.config.get("nextflow", {})
    for k, v in nextflow_params.items():
        config_raw = config_raw.replace(f"<{k}>", v)
    with open(work_dir/"workflow.config.nf", "w") as f:
        f.write(config_raw)

    # bootstrap
    bootstrap_path = AgentPaths.HOME_ROOT/"lib/msm_bootstrap"
    Log.Info(f"preparing entrypoint [{bootstrap_path.name}]")
    shutil.copy(bootstrap_path, work_internals)

    _rel = f"{extern_work}".replace(f"{extern_root}/", "")
    Log.Info(f"[{task.plan._key}] staged to [{_rel}]")
    if view:
        Log.Info(f"contents after staging:")
        with LiveShell() as shell:
            shell.RegisterOnOut(Log.Info)
            shell.RegisterOnErr(Log.Error)
            shell.Exec(f"cd {work_dir} && find .")

def ExecuteWorkflow(key: str):
    task_path = AgentPaths.to_task(key)
    workspace = task_path.parent.parent
    assert workspace.exists(), f"plan folder not found [{workspace}]"
    task = WorkflowTask.Load(task_path)
    Log.Info(f"executing workflow [{task.plan._key}] with [{len(task.plan.steps)}] steps")

    agent = Agent.Load(AgentPaths.HOME_ROOT/"lib/agent.yml")
    extern_home = agent.home.GetPath()
    extern_nxf_exe = extern_home/"lib/nextflow"

    Log.Info(f"connecting to relay for external shell")
    with RemoteShell(AgentPaths.to_relay_coms()) as extern_shell:
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