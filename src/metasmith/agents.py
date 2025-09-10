from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass, field
import tempfile
import shutil
from typing import Iterable, Literal
import yaml
import time
import re

from .serialization import StdTime
from .hashing import KeyGenerator
from .coms.ipc import LiveShell, ShellResult, RemoveLeadingIndent
from .logging import Log
from .coms.containers import Container, ContainerRuntime
from .coms.ipc import RemoteShell
from .models.remote import GlobusSource, Logistics, Source, SourceType, SshSource
from .models.workflow import WorkflowStep, WorkflowPlan, WorkflowTarget, WorkflowTask, NextflowGenContext
from .models.libraries import DataInstanceLibrary, DataInstance, DataTypeLibrary, TransformInstanceLibrary, TransformInstance
from .models.solver import Endpoint

class AgentPaths:
    WORK_ROOT = Path("/ws")
    HOME_ROOT = Path("/msm_home")
    INTERNALS = Path("_metasmith")
    STAGED = Path("runs")
    TASK = Path("task")

    @classmethod
    def to_staged(cls, root: Path=None):
        if root is None: root = cls.HOME_ROOT
        return root/cls.STAGED

    @classmethod
    def to_task(cls, key: str, root: Path=None):
        if root is None: root = cls.HOME_ROOT
        return root/(cls.STAGED/key)/cls.INTERNALS/cls.TASK

    @classmethod
    def to_bootstrap(cls, root: Path=None):
        if root is None: root = cls.HOME_ROOT
        return root/"lib/msm_bootstrap"

    @classmethod
    def to_definition(cls, root: Path=None):
        if root is None: root = cls.HOME_ROOT
        return root/"lib/agent.yml"

    @classmethod
    def to_relay(cls, root: Path=None):
        if root is None: root = cls.HOME_ROOT
        return root/"relay/msm_relay"

    @classmethod
    def to_relay_coms(cls, root: Path=None):
        return cls.to_relay(root).parent/"connections/main.in"

    @classmethod
    def to_data(cls, root: Path=None):
        if root is None: root = cls.HOME_ROOT
        return root/"data"

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
        Log.Info(f"connecting to deployed agent")
        self.agent._run_setup(self.shell)
        self.shell.RegisterOnOut(_on_out)
        self.shell.RegisterOnErr(_on_err)
        self.shell.Exec(f"cd {agent.home.GetPath()}")
        Log.Info(f"starting relay service")
        self.shell.Exec(f'./relay/msm_relay start')

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
    globus_uuid: str = None

    def _is_ssh(self):
        return self.home.type == SourceType.SSH

    def Pack(self):
        optional = {k:v for k, v in dict(
            globus_uuid=self.globus_uuid,
        ).items() if v is not None}
        return dict(
            setup_commands=list(self.setup_commands),
            home=self.home.Pack(),
            container=self.container,
        ) | optional

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
            ssh_src = SshSource.Parse(self.home.address)
            Log.Info(f"starting ssh to [{ssh_src.host}]")
            shell.Exec(f"ssh {ssh_src.host}")
            SUCCESS = f"ssh_connected_flag.{KeyGenerator.FromInt(2**42)}"
            on_out = lambda x: (Log.Info(f"{x}") if SUCCESS not in x else None)
            on_err = lambda x: Log.Error(f"{x}")
            shell.RegisterOnOut(on_out)
            shell.RegisterOnErr(on_err)
            res = shell.Exec(f'[ ! -z "$SSH_CONNECTION" ] && echo "{SUCCESS}"', timeout=timeout, history=True)
            shell.RemoveOnOut(on_out)
            shell.RemoveOnErr(on_err)
            if not any(SUCCESS in x for x in res.out):
                assert False, f"ssh connection failed {res.err}"

        for cmd in self.setup_commands:
            shell.Exec(cmd, timeout=timeout)

    def _run_cleanup(self, shell: LiveShell):
        pass

    def Deploy(self):
        with LiveShell() as shell, tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            _paused = False
            shell.RegisterOnOut(lambda x: (Log.Info(x) if not _paused else None))
            shell.RegisterOnErr(lambda x: (Log.Error(x) if not _paused else None))
            class PausedShell():
                def __enter__(self):
                    nonlocal _paused
                    _paused = True
                def __exit__(self, exc_type, exc_val, exc_tb):
                    nonlocal _paused
                    _paused = False

            def do_step(cmd: str, display_cmd: str=None, timeout=15):
                if display_cmd is not None: Log.Info(f">>> {display_cmd}")
                str_cmd = RemoveLeadingIndent(cmd)
                for x in str_cmd.split("\n"):
                    if display_cmd is None: Log.Info(f">>> {x}")
                return shell.Exec(cmd, timeout=timeout, history=True)

            _staged = []
            def _remote_file(x: str|Path, dest: Path, executable=False):
                if not isinstance(dest, Path): dest = Path(dest)
                assert not dest.is_absolute() or dest.is_relative_to(self.home.GetPath()), f"dest [{dest}] must be relative to [{self.home.GetPath()}]"
                (tmpdir/dest).parent.mkdir(parents=True, exist_ok=True)
                if isinstance(x, str):
                    x = RemoveLeadingIndent(x)
                    fpath = tmpdir/dest
                    with open(fpath, "w") as f:
                        f.write(x)
                    if executable: os.chmod(fpath, 0o755)
                else:
                    shutil.copytree(x, tmpdir/dest)
                _staged.append(dest)
                Log.Info(f"staged [{dest}]")

            def _sync_remote_files():
                mover = Logistics()
                mover.QueueTransfer(
                    src=Source.FromLocal(tmpdir),
                    dest=self.home,
                )
                Log.Info(f"deploying [{len(_staged)}] staged files")
                res = mover.ExecuteTransfers()
                assert len(res.completed) == 1, f"failed to deploy files"

            self._run_setup(shell)
            shell.Exec(f"mkdir -p {self.home.GetPath()}")
            with PausedShell():
                res = shell.Exec(f"""
                    realpath {self.home.GetPath()}
                    realpath ~
                """, history=True)
            resolved_agent_home, resolved_home = [Path(x.strip()) for x in res.out]

            dev_src = "$AGENT_HOME/dev/metasmith"
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
                container_cache=resolved_agent_home, # just so the main container is saved here
                binds=[
                    ("$AGENT_HOME", Path("/msm_home")),
                    (Path(resolved_home)/".globus", Path(resolved_home)/".globus"),
                    (Path(resolved_home)/".globusonline", Path(resolved_home)/".globusonline"),
                ],
                runtime=ContainerRuntime.APPTAINER
            )
            container_dev = make_dev_container(container)
            _cmds = [f"AGENT_HOME={resolved_agent_home}"]+[f"mkdir -p {p}" for p, _ in container.binds]
            do_step("\n".join(_cmds))
            _pull_cmd = container.MakePullCommand()
            do_step(
                cmd=f"[ -e {container._get_local_path()} ] || {_pull_cmd}",
                display_cmd=f"{{if not exists}}: {_pull_cmd.replace(' '+str(resolved_agent_home), '')}",
                timeout=None
            )

            _remote_file(
                f"""
                #!/bin/bash
                AGENT_HOME={resolved_agent_home}
                if [ -e "{dev_src}" ]; then
                    echo "including dev binds"
                    {container_dev.MakeRunCommand(local=f"$AGENT_HOME/metasmith.sif")} $@
                else
                    {container.MakeRunCommand(local=f"$AGENT_HOME/metasmith.sif")} $@
                fi
                """,
                dest="msm_stub",
                executable=True,
            )

            HERE='$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )'
            _remote_file(
                f"""
                #!/bin/bash
                HERE={HERE}
                $HERE/msm_stub metasmith $@
                """,
                dest="msm",
                executable=True,
            )

            _remote_copy = Agent(**self.Pack())
            _remote_copy.home = Source.FromLocal(resolved_agent_home)
            _remote_file(
                yaml.dump(_remote_copy.Pack()),
                dest=AgentPaths.to_definition(Path(".")),
            )

            bootstrap_container = Container(
                image=self.container,
                binds=[
                    (Path("./"), Path("/ws")),
                    ("$AGENT_HOME", Path("/msm_home")),
                ],
                workdir=Path("/ws"),
                runtime=ContainerRuntime.APPTAINER,
            )
            bootstrap_container_dev = make_dev_container(bootstrap_container)
            _remote_file(
                f"""
                #!/bin/bash

                AGENT_HOME={resolved_agent_home}
                TASK_DIR=$1
                STEP=$2
                CWD=${{3:-$(pwd -P)}}
                cd $CWD
                if [ -e "{AgentPaths.HOME_ROOT}" ]; then
                    echo "bootstrap called from container, bouncing to external [$@]"
                    REL_CWD=$(realpath --relative-to="{AgentPaths.HOME_ROOT}" $CWD)
                    CMD="{AgentPaths.to_bootstrap(Path('$AGENT_HOME'))} $@ $AGENT_HOME/$REL_CWD"
                    {AgentPaths.to_relay()} bounce "$CMD"
                    exit
                fi

                echo "bootstrap ======================"
                INTERNALS="_metasmith"
                [ -z $STEP ] && echo "no step provided" && exit 1
                echo "cwd [$(pwd -P)]"
                echo "task [$TASK_DIR]"
                echo "step [$STEP]"
                function run_container {{
                    if [ -e "{dev_src}" ]; then
                        echo "including dev binds"
                        {bootstrap_container_dev.MakeRunCommand(local=f"$AGENT_HOME/metasmith.sif")} $@
                    else
                        {bootstrap_container.MakeRunCommand(local=f"$AGENT_HOME/metasmith.sif")} $@
                    fi
                }}
                echo "deploy relay ==================="
                run_container metasmith api deploy_from_container -a workspace=$INTERNALS
                find $INTERNALS/relay/
                echo "pre execute ===================="
                find .
                ls -lh .
                echo "relay =========================="
                $INTERNALS/relay/msm_relay start --channels 3
                echo "execute ========================"
                run_container metasmith api execute_transform -a step_index=$STEP -a workspace=$TASK_DIR
                echo "post execute ==================="
                find .
                ls -lh .
                echo "cleanup ========================"
                $INTERNALS/relay/msm_relay stop
                """,
                dest=AgentPaths.to_bootstrap(Path(".")),
                executable=True,
            )

            HERE = Path(__file__).parent
            _remote_file(HERE/"nextflow_config", "lib/nextflow_config")
            _sync_remote_files()

            do_step(f"cd {resolved_agent_home} && ./msm api deploy_from_container")

            self._run_cleanup(shell)
            Log.Info(f"deployed to [{self.home.address}]")

    def GenerateWorkflow(
        self, given: Iterable[DataInstanceLibrary], transforms: Iterable[TransformInstanceLibrary], targets: Iterable[Endpoint],
        config: dict|None=None,
        max_iter: int=1024, max_refine: int=256, seed: int=42,
    ):
        plan = WorkflowPlan.Generate(given, transforms, targets, max_iter=max_iter, max_refine=max_refine, seed=seed)
        if config is None: config = {}
        task = WorkflowTask(plans=[plan], data_libraries=list(given),transform_libraries=list(transforms), config=config)
        return task

    def StageWorkflow(self, task: WorkflowTask, on_exist: str = "skip"):
        assert on_exist in {"skip", "error", "clear", "update"}
        agent_shell = AgentShell(self)
        with agent_shell as sh_remote:
            remote_path = AgentPaths.to_task(task._key, root=self.home.GetPath())
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
                        Log.Warn(f"clearing previously staged task")
                        _to_delete_src = remote_work_path
                        _to_delete = _to_delete_src.with_suffix(".to_delete")
                        sh_remote.Exec(f"mv {_to_delete_src} {_to_delete} && rm -rf {_to_delete}")
                    elif on_exist == "update":
                        Log.Warn(f"updating previously staged task")

            Log.Info(f"sending metadata for workflow [{task._key}]")
            task.SaveAs(self.home.ReplacePathWith(remote_path))
            Log.Info(f"staging")
            sh_remote.Exec(f"./msm api stage_workflow -a task_key={task._key}")

    def RunWorkflow(self, task: WorkflowTask|str):
        key = task._key if isinstance(task, WorkflowTask) else str(task)
        agent_shell = AgentShell(self)
        with agent_shell as sh_remote:
            Log.Info(f"triggering execution of [{key}]")
            task_path = AgentPaths.to_task(key, root=self.home.GetPath())
            workspace = task_path.parent.parent
            FLAG = "workspace exists"
            with PausedShell(agent_shell): # this syntax is confusing, need to fix
                res = sh_remote.Exec(f"[ -e {workspace} ] && echo '{FLAG}'", history=True)
            assert FLAG in res.out, f"task not staged, expected [{workspace}] to exist"
            LOG_DIR = Path(f"{AgentPaths.INTERNALS}/logs.{StdTime.Timestamp()}") # this timestamp is used as the start time below!
            launcher_log = workspace/LOG_DIR/"main.raw.log"
            sh_remote.Exec(
                f"""
                mkdir -p {launcher_log.parent}
                nohup ./msm api run_workflow -a key={key} -a log_dir={LOG_DIR} >{launcher_log} 2>&1 &
                """,
            )

    def CheckWorkflow(self, task: WorkflowTask|str, run: int=None):
        key = task._key if isinstance(task, WorkflowTask) else str(task)
        with AgentShell(self) as sh_remote:
            index_param = "" # 1 indexed
            if run is not None:
                index_param = f"-a index={run}"
            sh_remote.Exec(f"./msm api check_workflow -a key={key} {index_param}")

    def GetResultSource(self, task: WorkflowTask|str, allow_globus: bool = True, check_exists: bool = False):
        key = task._key if isinstance(task, WorkflowTask) else str(task)
        result_path = AgentPaths.to_staged(root=self.home.GetPath())/f"{key}/results"
        if check_exists:
            agent_shell = AgentShell(self)
            with agent_shell as sh_remote:
                FLAG = "results exist"
                with PausedShell(agent_shell):
                    res = sh_remote.Exec(f"[ -e {result_path} ] && echo '{FLAG}'", history=True)
                assert FLAG in res.out, f"results not found at [{self.home.ReplacePathWith(result_path).address}]"

        if self.globus_uuid is not None and allow_globus:
            src = GlobusSource(endpoint=self.globus_uuid, path=result_path).AsSource()
        else:
            src = self.home.ReplacePathWith(result_path)
        return src

# ===========================================================================
# calls to staged Agent

_get_nextflow_preset = lambda config: config.get("nextflow", {}).get("preset", "default")

def StageWorkflow(task_key: str):
    agent = Agent.Load(AgentPaths.HOME_ROOT/"lib/agent.yml")
    task_path = agent.home.GetPath()/AgentPaths.to_task(task_key)
    assert task_path.exists(), f"task dir not found [{task_path}]"
    task = WorkflowTask.Load(task_path)
    Log.Info(f"staging workflow [{task._key}] with [{len(task.data_libraries)}] data libs and [{len(task.transform_libraries)}] transform libs")

    work_relative = AgentPaths.STAGED/task._key
    work_dir = AgentPaths.WORK_ROOT/work_relative
    work_internals = work_dir/AgentPaths.INTERNALS
    data_dir = AgentPaths.to_data()
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

    # data libraries
    Log.Info(f"moving remote data libraries to [{data_dir}]")
    def move_remote_libs(libs: list[DataInstanceLibrary], dest: Path):
        processed_libs: list[DataInstanceLibrary] = []
        mover = Logistics()
        expected: list[Source] = []
        for lib in libs:
            if lib.remote_src is not None:
                lib_dest = dest/lib.location.name
                if not lib_dest.exists():
                    _dest = Source.FromLocal(lib_dest)
                    lib.PrepTransfer(_dest, mover=mover)
                    expected.append(_dest)
                lib.location = lib_dest
            processed_libs.append(lib)
        res = mover.ExecuteTransfers()
        _completed = {b.address for a, b in res.completed}
        for x in expected:
            assert x.address in _completed, f"failed to transfer [{x.address}]"
        return processed_libs
    task.data_libraries = move_remote_libs(task.data_libraries, data_dir)

    # nextflow
    task.PrepareNextflow(NextflowGenContext(
        work_dir=work_dir,
        external_work=extern_work,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=agent.home.GetPath(),
    ))
    nextflow_config_dir = AgentPaths.HOME_ROOT/"lib/nextflow_config"
    nextflow_preset = _get_nextflow_preset(task.config)
    preset_path = nextflow_config_dir/f"{nextflow_preset}.nf"
    if not preset_path.exists():
        Log.Warn(f"nextflow preset not found [{preset_path}], using default")
        preset_path = nextflow_config_dir/"default.nf"
    else:
        Log.Info(f"using nextflow preset [{preset_path.stem}]")
    with open(preset_path) as f:
        config_raw = "".join(f.readlines())
    nextflow_params = dict(
        cpus=4, memory="16 GB", time="3h",
        queueSize=100, submitRateLimit="10/1sec", pollInterval="10sec", stageInMode="symlink",
    )|task.config.get("nextflow", {})
    for k, v in nextflow_params.items():
        Log.Info(f"setting nextflow param [{k}] from config") # don't show in case sensitive values
        config_raw = config_raw.replace(f"<{k}>", str(v))
    with open(work_dir/"workflow.config.nf", "w") as f:
        f.write(config_raw)

    _rel = f"{extern_work}".replace(f"{extern_root}/", "")
    Log.Info(f"[{task._key}] staged to [{{AGENT_HOME}}/{_rel}]")

def RunWorkflow(key: str, log_dir: Path):
    task_path = AgentPaths.to_task(key)
    workspace = task_path.parent.parent
    assert workspace.exists(), f"task workspace not found [{workspace}]"

    task = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])
    nextflow_preset = _get_nextflow_preset(task.config)
    # start_time = StdTime.Timestamp()
    start_time = log_dir.name.split(".")[-1]
    Log.Info(f"start time [{start_time}]")
    Log.Info(f"running workflow [{task._key}] with preset [{nextflow_preset}]")

    Log.Info(f"loading agent metadata")
    agent = Agent.Load(AgentPaths.to_definition())
    extern_home = agent.home.GetPath()
    extern_workspace = AgentPaths.to_task(key, root=extern_home).parent.parent
    (workspace/log_dir).mkdir(parents=True, exist_ok=True)
    MAIN_LOG = workspace/log_dir/"main.log"
    Log.AddLogFile(MAIN_LOG)

    Log.Info(f"workspace [{workspace}]")
    Log.Info(f"external workspace [{extern_workspace}]")
    Log.Info(f"preset [{nextflow_preset}]")
    Log.Info(f"plans [{len(task.plans)}] | steps [{sum(len(p.steps) for p in task.plans)}]")

    if agent.globus_uuid is not None:
        Log.Info(f"locating input data with agent's globus endpoint [{agent.globus_uuid}]")
        dest_base = GlobusSource(endpoint=agent.globus_uuid, path="/").AsSource()
    else:
        Log.Info(f"locating input data with personal globus endpoint")
        dest_base = Source.FromLocal("/")
    for lib in task.transform_libraries+task.data_libraries:
        _name = lib.location.name
        if lib.remote_src is None:
            Log.Info(f"[{_name}] is at [{lib.location}]")
        else:
            _extern_location = str(lib.location).replace(str(AgentPaths.HOME_ROOT), str(extern_home))
            Log.Info(f"[{_name}] at [{lib.location}] is remote [{lib.remote_src.address}], downloading to [{_extern_location}]")
            dest = dest_base/_extern_location
            lib.Actualize(extern_dest=dest, label=f"msm_staging.{_name}")

    # need to call nf inside container
    # nf needs java and is not a standalone executable
    with LiveShell() as shell:
        shell.RegisterOnOut(Log.Info)
        shell.RegisterOnErr(Log.Error)
        Log.Info(f"calling nextflow from container")
        shell.Exec(
            f"""
            cd {workspace}
            export NXF_HOME=./.nextflow
            nextflow -c ./workflow.config.nf \
                -log {log_dir}/nxf.log \
                run ./workflow.nf \
                -resume \
                -work-dir ./nxf_work
            """,
            timeout=None,
        )

    Log.Info(f"compiling results")
    results_folder = "results/latest"
    output_path = workspace/results_folder
    extern_output_path = extern_workspace/results_folder
    output = DataInstanceLibrary(output_path)
    type_libs: dict[str, DataTypeLibrary] = {}
    for lib in task.transform_libraries:
        type_libs.update(lib.types)
    used_type_libs = set()
    def _get_target_path(target: WorkflowTarget):
        p = f"{target.producing_step.order:08}/{target.instance.path}"
        return output_path/p, extern_output_path/p
    produced_targets: list[WorkflowTarget] = []
    for target in [t for p in task.plans for t in p.targets]:
        inst = target.instance
        p, ex_p = _get_target_path(target)
        if not p.exists():
            Log.Error(f"workflow failed to produce expected output [{inst.dtype_name}] at [{ex_p}]")
            continue
        produced_targets.append(target)
        _namespace, _ = inst.GetDType()
        used_type_libs.add(_namespace)
        for p in target.used_givens:
            for _namespace, lib in p.parent_lib.types.items():
                if _namespace in used_type_libs: continue
                used_type_libs.add(_namespace)
                type_libs[_namespace] = lib
    to_add = []
    parent_map: dict[Path, list[DataInstance]] = {}
    for target in produced_targets:
        inst = target.instance
        p, ex_p = _get_target_path(target)
        rel_p = p.relative_to(output_path)
        to_add.append([p, rel_p, inst.dtype_name])
        parent_map[rel_p] = target.used_givens
    for _namespace in used_type_libs:
        output.AddTypeLibrary(_namespace, type_libs[_namespace])
    output.Add(items=to_add, method=SourceType.DIRECT, on_exist="skip")
    for e_path, parents in parent_map.items():
        output.AddParentsTo(e_path, parents)
    output.Save()

    tail = output_path.relative_to(AgentPaths.HOME_ROOT)
    external_results_path = extern_home/tail
    Log.Info(f"results for [{key}] at [{external_results_path}]")

    Log.Info(f"gathering log files")
    nxf_ids = set()
    nxf_id_len = 9 # 2 + "/" + 6
    with open(MAIN_LOG, "r") as f:
        for l in f:
            candidates = re.findall(r"[\dabcdef]{2}/[\dabcdef]{6}\]", l)
            if len(candidates) == 0: continue
            hit = candidates[0]
            nxf_id = hit[:nxf_id_len]
            nxf_ids.add(nxf_id)
    NXF_WORK = workspace/"nxf_work"
    PROCESS_DEST = workspace/log_dir/"steps"
    PROCESS_DEST.mkdir(parents=True, exist_ok=True)
    for p in NXF_WORK.glob("*/*"):
        p = p.relative_to(NXF_WORK)
        nxf_id = str(p)[:nxf_id_len]
        if nxf_id not in nxf_ids: continue
        name = nxf_id
        with open(NXF_WORK/p/".command.run") as f:
            for i, l in enumerate(f):
                if i < 2: continue
                # example |### name: 'b0000:i0002:s00000003_trimmomatic__hb4OBV15 (1)'|
                name = l.split("'")[1].split(" ")[0].replace(":", "-")
                break
        dest = PROCESS_DEST/f"{name}.log"
        src = NXF_WORK/p/".command.log"
        if not src.exists():
            Log.Warn(f"no log found for [{name}:{p}]")
            continue
        shutil.copy2(src, dest)

    output_path = output_path.rename(output_path.parent/start_time)
    Log.Info(f"linking logs to results folder [{output_path}]")
    output_metadata_path = output_path/f"{output._path_to_meta}"
    (output_metadata_path/"logs").symlink_to(f"../../../{log_dir}")
    Log.Info(f"run completed at [{StdTime.Timestamp()}]")

def CheckWorkflow(key: str, index: int=None):
    task_path = AgentPaths.to_task(key)
    workspace = task_path.parent.parent
    assert workspace.exists(), f"task workspace not found [{workspace}], maybe it wasn't staged yet"

    Log.Info(f"searching for logs")
    internals = workspace/AgentPaths.INTERNALS
    log_dirs = list((internals).glob("logs.*"))
    log_dirs = sorted(log_dirs, key=lambda x: x.name)
    if len(log_dirs) == 0:
        Log.Warn(f"no logs found for [{key}]")
        return
    Log.Info(f"found [{len(log_dirs)}] runs")
    for i, log_entry in enumerate(log_dirs):
        n_str = f"{i+1}"
        Log.Info(f"{' '*(5-len(n_str))}{n_str}: [{log_entry.name}]")

    log_dir = log_dirs[-1]
    msg = f"here is the main log of the latest run [{log_dir.name}]"
    if index is not None:
        if index < 1 or index > len(log_dirs):
            Log.Warn(f"index [{index}] out of range")
        else:
            log_dir = log_dirs[index-1]
            msg = f"here is the main log for run [{index}] [{log_dir.name}]"

    Log.Info(msg)
    Log.Info(f">"*len(msg))
    Log.Info("")
    
    with open(workspace/log_dir/"main.raw.log", "r") as f:
        lines = f.readlines()
        MAXL = 1000
        HEAD = 20
        if len(lines)>1000:
            print("".join(lines[:HEAD]))
            print(f"... +{len(lines)-HEAD-MAXL}")
            print("".join(lines[-(MAXL-HEAD):]))
        else:
            print("".join(lines))

    Log.Info("")
    Log.Info(f"<"*len(msg))
    Log.Info(f"log folder at [{workspace/log_dir}]")
