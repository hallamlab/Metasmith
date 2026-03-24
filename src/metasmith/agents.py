from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass, field
import tempfile
import shutil
from typing import Iterable, Literal
import yaml
import json
import re
from hashlib import md5
import pandas as pd
from glob import glob

from .serialization import StdTime
from .hashing import KeyGenerator
from .logging import Log
from .coms.containers import Container, ContainerRuntime
from .coms.terminals import LiveShell, ShellResult, RemoveLeadingIndent
from .coms.via_file_watcher import RemoteShell
from .models.remote import GlobusSource, Logistics, Source, SourceType, SshSource
from .models.workflow import METADATA_FILE, WorkflowStep, WorkflowPlan, WorkflowTarget, WorkflowTask, NextflowGenContext, BIND_FILE
from .models.libraries import DataInstanceLibrary, DataInstance, DataTypeLibrary, TransformInstanceLibrary, DataInstanceLibraryView
from .models.libraries import TransformInstance, Resources
from .models.solver import Dependency, Endpoint, Solution, Transform
from .constants import VERSION, MODULE_PATH, AgentPaths

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
            try:
                self.shell.Exec("exit", timeout=5)
            except (KeyboardInterrupt, TimeoutError):
                pass
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

class TargetBuilder:
    def __init__(self) -> None:
        self.targets: dict[str, set[str]] = {}

    def Add(self, target_type: str, parents: set[str]|None=None):
        if parents is None: parents = set()
        assert "::" in target_type, f'expected @type to in the form of "namespace::type_name" but got [{target_type}]'
        for p in parents:
            assert p in self.targets, f'[{p}] needs to be added before use as a parent'
        assert target_type not in self.targets, f'[{target_type}] already added'
        self.targets[target_type] = parents.copy()
        return target_type

ResourceOverrides = dict[int|Literal["all"]|Literal["*"]|str|TransformInstance, Resources]
@dataclass
class Agent:
    home: Source
    setup_commands: list[str] = field(default_factory=list)
    container: str = f"docker://quay.io/hallamlab/metasmith:{VERSION}"
    globus_uuid: str|None = None
    runtime: ContainerRuntime=ContainerRuntime.APPTAINER
    real_path: Path|None = None

    def _is_ssh(self):
        return self.home.type == SourceType.SSH

    def Pack(self) -> dict:
        optional = {k:str(v) for k, v in dict(
            globus_uuid=self.globus_uuid,
            real_path=self.real_path,
        ).items() if v is not None}
        # if isinstance(self.runtime, str): print(f"##### [{self.runtime}]")
        return dict(
            setup_commands=list(self.setup_commands),
            home=self.home.Pack(),
            container=self.container,
            runtime=self.runtime.name,
        ) | optional

    def Save(self, file_path: Path):
        with open(file_path, "w") as f:
            yaml.dump(self.Pack(), f)

    @classmethod
    def Unpack(cls, data):
        data["home"] = Source.Unpack(data["home"])
        data["runtime"] = ContainerRuntime[data["runtime"]]
        k = "real_path"
        if k in data:
            data[k] = Path(data[k])
        return cls(**data)

    @classmethod
    def Load(cls, file_path: Path):
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
        return cls.Unpack(data)
    
    def _get_realpath(self):
        """realpath is resolved upon deployment"""
        assert self.real_path is not None, "not resolved"
        return self.real_path

    def _run_setup(self, shell: LiveShell, timeout: int|None = None):
        if self._is_ssh():
            ssh_src = SshSource.Parse(self.home.address)
            Log.Info(f"starting ssh to [{ssh_src.host}]")
            shell.Exec(f"ssh {ssh_src.host}")
            SUCCESS = f"ssh_connected_flag.{KeyGenerator.FromInt(2**42)}"
            def on_out(x):
                if SUCCESS in x: return
                Log.Info(f"{x}")
            def on_err(x):
                Log.Error(f"{x}")
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

    def Deploy(self, assertive: bool=False):
        Log.Info(f"deploying agent version [{VERSION}] to [{self.home.address}]")
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

            def do_step(cmd: str, display_cmd: str|None=None, timeout:float|None=15):
                if display_cmd is not None: Log.Info(f">>> {display_cmd}")
                str_cmd = RemoveLeadingIndent(cmd)
                for x in str_cmd.split("\n"):
                    if display_cmd is None: Log.Info(f">>> {x}")
                return shell.Exec(cmd, timeout=timeout, history=True)

            _staged = []
            def _remote_file(x: str|Path, dest: str|Path, executable=False):
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
                for e in res.errors:
                    Log.Error(e)
                assert len(res.completed) == 1, f"failed to deploy files"

            with PausedShell():
                self._run_setup(shell)
                _FLAG = "already exists"
                res = shell.Exec(f'[[ -e "{self.home.GetPath()}" ]] && echo "{_FLAG}"', history=True)
                if _FLAG in res.out and not assertive: 
                    Log.Info(f"[{self.home.address}] already exists, use Deploy(assertive=True) to deploy anyways")
                    return

            shell.Exec(f'mkdir -p "{self.home.GetPath()}"')
            with PausedShell():
                cmds = [
                    f'realpath {self.home.GetPath()}',
                    f'realpath ~',
                    f'hostname',
                ]
                res = shell.Exec('\n'.join(cmds), history=True)
            assert len(res.out)==len(cmds), res.out
            resolved_agent_home, resolved_home, hostname = [x.strip() for x in res.out]
            resolved_agent_home = Path(resolved_agent_home)
            resolved_home = Path(resolved_home)

            dev_src = "$AGENT_HOME/dev/metasmith"
            dev_mock = Container(
                image=self.container,
                binds=[
                    (dev_src, Path("/opt/conda/envs/metasmith_env/lib/python3.12/site-packages/metasmith")),
                ],
                runtime=self.runtime,
            )

            container = Container(
                image=self.container,
                container_cache=Path("$AGENT_HOME")/AgentPaths.CONTAINER_CACHE,
                binds=[
                    ("$(pwd -P)", Path("/ws")),
                    ("$AGENT_HOME", Path("/msm_home")),
                    ('${TMPDIR-"/tmp"}', '${TMPDIR-"/tmp"}'),
                    (resolved_home/".globus", resolved_home/".globus"),
                    (resolved_home/".globusonline", resolved_home/".globusonline"),
                ],
                runtime=self.runtime,
            )
            _cmds = [
                f"AGENT_HOME={resolved_agent_home}"
            ] + [
                f"mkdir -p {p}" for p, _ in container.binds
            ]
            do_step("\n".join(_cmds))
            _local_path = container.GetLocalPath()
            if _local_path:
                _pull_cmd = container.MakePullCommand()
                do_step(
                    cmd=f'mkdir -p "{_local_path.parent}" && [ -e {_local_path} ] || {_pull_cmd}',
                    display_cmd=f"{{if not exists}}: {_pull_cmd.replace(str(resolved_agent_home), '$AGENT_HOME')}",
                    timeout=None
                )

            _remote_file(
                f"""
                #!/bin/bash
                AGENT_HOME={resolved_agent_home}
                BINDS="$BINDS {container.MakeBindsParam()}"
                if [ -e "{dev_src}" ]; then
                    echo "including dev binds"
                    BINDS="$BINDS {dev_mock.MakeBindsParam()}"
                fi
                echo "binds [$BINDS]"
                {container.MakeRunCommand(local=True, custom_bind_param="$BINDS")} metasmith $@
                """,
                dest="msm",
                executable=True,
            )

            _remote_copy = Agent.Unpack(self.Pack())
            _remote_copy.home = Source.FromLocal(resolved_agent_home)
            _remote_copy.real_path = resolved_agent_home
            _remote_file(
                yaml.dump(_remote_copy.Pack()),
                dest=AgentPaths.to_definition(Path(".")),
            )

            bootstrap_container = Container(
                image=self.container,
                binds=[
                    ("$(pwd -P)", Path("/ws")),
                    ("$AGENT_HOME", Path("/msm_home")),
                ],
                workdir=Path("/ws"),
                runtime=self.runtime,
                container_cache=Path("$AGENT_HOME")/AgentPaths.CONTAINER_CACHE
            )
            _remote_file(
                f"""
                #!/bin/bash

                AGENT_HOME={resolved_agent_home}
                TASK_DIR=$1
                STEP=$2
                HOST_NAME=$3
                CWD=${{4:-$(pwd -P)}}
                cd $CWD
                if [ -e "{AgentPaths.HOME_ROOT}" ]; then
                    echo "bootstrap called from container, bouncing to external [$@]"
                    REL_CWD=$(realpath --relative-to="{AgentPaths.HOME_ROOT}" $CWD)
                    CMD="{AgentPaths.to_bootstrap(Path('$AGENT_HOME'))} $@ $AGENT_HOME/$REL_CWD"
                    /app/msm_relay.x86_64-linux --io {AgentPaths.to_relay().parent}/$HOST_NAME bounce "$CMD"
                    exit
                fi

                echo "bootstrap ======================"
                INTERNALS="_metasmith"
                [ -z $STEP ] && echo "no step provided" && exit 1
                echo "cwd [$(pwd -P)]"
                echo "task [$TASK_DIR]"
                echo "step [$STEP]"
                function run_container {{
                    BINDS="{bootstrap_container.MakeBindsParam()}"
                    if [ -e "{dev_src}" ]; then
                        echo "including dev binds"
                        BINDS="$BINDS {dev_mock.MakeBindsParam()}"
                    fi
                    if [ -e "./{BIND_FILE}" ]; then
                        echo "including linked data binds"
                        BINDS="$BINDS $(cat ./{BIND_FILE})"
                    fi
                    echo "final binds:"
                    echo "$BINDS"
                    {bootstrap_container.MakeRunCommand(local=True, custom_bind_param="$BINDS")} $@
                }}
                echo "deploy relay ==================="
                run_container metasmith api deploy_from_container -a workspace=$INTERNALS architecture=$(uname -m) system=$(uname -s)
                find $INTERNALS/relay/
                echo "pre execute ===================="
                find .
                ls -lh .
                echo "relay =========================="
                $INTERNALS/relay/msm_relay start --local
                echo "execute ========================"
                run_container metasmith api execute_transform -a step_index=$STEP -a workspace=$TASK_DIR host=$(hostname)
                echo "post execute ==================="
                find .
                ls -lh .
                echo "cleanup ========================"
                $INTERNALS/relay/msm_relay stop
                echo "relay logs ====================="
                $INTERNALS/relay/msm_relay logs
                """,
                dest=AgentPaths.to_bootstrap(Path(".")),
                executable=True,
            )

            _sync_remote_files()
            do_step(f"{resolved_agent_home}/msm api deploy_from_container -a workspace={AgentPaths.HOME_ROOT} architecture=$(uname -m) system=$(uname -s)")
            self._run_cleanup(shell)
            Log.Info(f"deployed to [{self.home.address}]")

    def GenerateWorkflow(
        self, 
        samples: Iterable[DataInstanceLibraryView|DataInstanceLibrary],
        resources: Iterable[DataInstanceLibraryView|DataInstanceLibrary],
        transforms: list[TransformInstanceLibrary],
        targets: TargetBuilder,
        max_iter: int=1024, max_refine: int=256, seed: int=42,
    ):
        assert len(targets.targets)>0, "[targets] can not be empty"
        
        def _get_endpoint(dtype_name: str):
            ns, _ = dtype_name.split("::")
            for trlib in transforms:
                if ns not in trlib.types: continue
                e = trlib.GetType(dtype_name)
                N = 3
                lpath = trlib.location
                if len(lpath.parts)>N:
                    loc = "..."+"/".join(lpath.parts[-3:])
                else:
                    loc = f"{lpath}"
                Log.Info(f"[{dtype_name}] resolved by [{loc}]")
                return e
            assert False, f"no transforms had the namespace [{ns}]"

        target_model = Transform()
        _dtname2dep: dict[str, Dependency] = {}
        target_names: dict[Endpoint, str] = {}
        for dtype_name, parents in targets.targets.items():
            e = _get_endpoint(dtype_name)
            assert e not in target_names, f"[{dtype_name}] is a duplicate of [{target_names[e]}]"
            d = target_model.AddRequirement(example=e, parents={_dtname2dep[p] for p in parents})
            _dtname2dep[dtype_name] = d
            target_names[e] = dtype_name

        res_views = [lib if isinstance(lib, DataInstanceLibraryView) else DataInstanceLibraryView(lib) for lib in resources]
        _samples = [sample if isinstance(sample, DataInstanceLibraryView) else DataInstanceLibraryView(sample) for sample in samples]
        gen_result = WorkflowPlan.Generate(
            given=[
                [sample]+res_views
                for sample in _samples
            ], 
            transforms=transforms,
            target_names=target_names,
            target_model=target_model,
            max_iter=max_iter, max_refine=max_refine, seed=seed
        )
        sample_libs = {v._original for v in _samples}
        if isinstance(gen_result, Solution):
            return WorkflowTask(ok=False, plan=WorkflowPlan(given=[], targets=[], steps=[], _solver_result=gen_result))
        else:
            orig_resources = [lib if isinstance(lib, DataInstanceLibrary) else lib._original for lib in resources]
            return WorkflowTask(ok=True, plan=gen_result, data_libraries=list(sample_libs)+orig_resources,transform_libraries=transforms)

    def _get_mock_container(self, task: WorkflowTask):
        binds = task.GetCommonInputFolders(method="external")
        mock = Container(
            image=self.container,
            binds=[
                (p, p)
                for p in binds
            ],
            runtime=self.runtime,
        )
        return mock

    def StageWorkflow(self, task: WorkflowTask, on_exist: str = "skip", verify_external_paths: bool=False):
        VALID_ON_EXIST = {"skip", "error", "clear", "update", "update_workflow", "update_data"}
        assert on_exist in VALID_ON_EXIST, f"on_exist option [{on_exist}] is not one of {VALID_ON_EXIST}"
        Log.Info(f"staging workflow [{task.GetKey()}]")
        agent_shell = AgentShell(self)
        task_stage_partial = False
        with agent_shell as sh_remote:
            remote_path = AgentPaths.to_task(task._key, root=self.home.GetPath())
            remote_work_path = remote_path.parent.parent
            with PausedShell(agent_shell):
                FLAG = "task already staged"
                res = sh_remote.Exec(f'[ -e {remote_work_path} ] && echo "{FLAG}"', history=True)
                if FLAG in res.out:
                    _msg = f"task already staged at [{remote_work_path}]"
                    if on_exist not in {"error"}:
                        Log.Warn(_msg)
                    match on_exist:
                        case "error":
                            raise FileExistsError(_msg)
                        case "skip":
                            return
                        case "clear":
                            Log.Warn(f"clearing previously staged task")
                            _to_delete_src = remote_work_path
                            _to_delete = _to_delete_src.with_suffix(".to_delete")
                            sh_remote.Exec(f"mv {_to_delete_src} {_to_delete} && rm -rf {_to_delete}")
                        case "update":
                            Log.Warn(f"updating previously staged task")
                        case "update_data":
                            Log.Warn(f"resending data for previously staged task")
                            task_stage_partial = "data_only"
                        case "update_workflow":
                            Log.Warn(f"recompiling workflow for previously staged task")
                            task_stage_partial = "transforms_only"

            Log.Info(f"sending context for workflow [{task._key}]")
            task.SaveAs(self.home.ReplacePathWith(remote_path), partial=task_stage_partial)
            Log.Info(f"staging")
            mock = self._get_mock_container(task)
            binds = mock.MakeBindsParam()
            if len(mock.binds)>0:
                Log.Info(f"external binds {[a for a, b in mock.binds]}")
            sh_remote.Exec(f"""\
                export BINDS="{binds}"
                ./msm api stage_workflow -a task_key={task._key} verify={verify_external_paths} host=$(hostname)
            """, timeout=None)

    def GetNxfConfigPresets(self, folder: Path = MODULE_PATH/"nextflow_config"):
        if not folder.exists(): raise FileNotFoundError(folder)
        presets: dict[str, Path] = {}
        for f in folder.iterdir():
            if f.is_dir(): continue
            if not f.name.endswith(".nf"): continue
            presets[f.stem] = f.absolute()
        return presets

    def RunWorkflow(
            self, 
            task: WorkflowTask|str, 
            config_file: Path|None=None, 
            params: dict|Path|str|None=None, 
            resource_overrides: ResourceOverrides|None=None,
            stub_delay: float=0,
        ) -> None:
        is_dry_run = stub_delay>0
        if is_dry_run:
            Log.Info(f"starting dry run")
        if config_file is None: config_file = self.GetNxfConfigPresets()["local"]
        task_key = task.GetKey() if isinstance(task, WorkflowTask) else task
        agent_shell = AgentShell(self)
        with agent_shell as sh_remote:
            task_path = AgentPaths.to_task(task_key, root=self.home.GetPath())
            workspace = task_path.parent.parent
            FLAG = "workspace exists"
            with PausedShell(agent_shell): # this syntax is confusing, need to fix
                res = sh_remote.Exec(f"[ -e {workspace} ] && echo '{FLAG}'", history=True)
            assert FLAG in res.out, f"task not staged, expected [{workspace}] to exist"

            Log.Info(f"sending config and params")
            mover = Logistics()
            rel_ws = workspace.relative_to(self.home.GetPath())
            ws_dest = self.home/rel_ws
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir = Path(temp_dir)
                # params
                if params is None:
                    params = dict(nothing=None)
                if isinstance(params, dict):
                    params_local = temp_dir/AgentPaths.NXF_PARAMS
                    # lets underscores signify nested dictionaries
                    # so "{process_tries=3}" becomes { process={ tries=3 } } 
                    def _parse(d: dict):
                        parsed = {}
                        for k, v in d.items():
                            k = str(k)
                            if isinstance(v, dict):
                                v = _parse(v)
                            if "_" in k:
                                stacks = [x for x in k.split("_") if x != ""]
                                if len(stacks)>1:
                                    _d_curr = parsed
                                    for k in stacks[:-1]:
                                        _d_curr[k] = {}
                                        _d_curr = _d_curr[k]
                                    _d_curr[stacks[-1]] = v
                            else:
                                parsed[k] = v
                        return parsed

                    with open(params_local, "w") as f:
                        yaml.safe_dump(_parse(params), f)
                    params_source = Source.FromLocal(params_local)
                elif isinstance(params, Path):
                    params_source = Source.FromLocal(params)
                mover.QueueTransfer(src=params_source, dest=ws_dest/AgentPaths.NXF_PARAMS)
                # resource overrides
                local_config = temp_dir/config_file.name
                shutil.copy(config_file, local_config)
                mover.QueueTransfer(src=Source.FromLocal(local_config), dest=ws_dest/AgentPaths.NXF_CONFIG)
                # lines = [
                #     # "",
                #     # "lineage.enabled = true",
                #     # "lineage.store.location = 'nxf_lineage'",
                # ]
                if resource_overrides is not None:
                    with open(local_config, "a") as f:
                        TAB="\t"
                        lines = [
                            "",
                            "process {"
                        ]
                        for tr, res in resource_overrides.items():
                            if tr=="all" or tr=="*":
                                key = f".*"
                            elif isinstance(tr, int):
                                p = tr
                                key = f"p{p:02}__.*"
                            elif isinstance(tr, str):
                                key = f".*__{tr}"
                            else:
                                key = f".*__{tr.name}"

                            if isinstance(res, Resources):
                                lines += [
                                    TAB+f"withName: '{key}' "+"{",
                                ]+[TAB+TAB+x for x in res.AsNextflowFormat(is_config=True)]+[
                                    TAB+"}",
                                ]
                            else:
                                raise TypeError(f"resouce specification in unexpected format: [{type(res)}]")
                        lines += [
                            "}",
                            "",
                        ]
                        f.write("\n".join(lines))
                mover.ExecuteTransfers(wait_for_complete=True)

            if is_dry_run:
                m = "dry run"
            else:
                m = "execution"
            Log.Info(f"triggering {m} of [{task_key}]")
            sh_remote.Exec(f"{workspace/AgentPaths.LAUNCHER_FILE} {stub_delay:0.3f}")

    def CheckWorkflow(self, task: WorkflowTask|str, run: int|None=None):
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

def StageWorkflow(task_key: str, verify: bool, host: str):
    agent = Agent.Load(AgentPaths.HOME_ROOT/"lib/agent.yml")
    task_path = agent.home.GetPath()/AgentPaths.to_task(task_key)
    assert task_path.exists(), f"task dir not found [{task_path}]"
    task = WorkflowTask.Load(task_path)
    Log.Info(f"staging workflow [{task._key}] with:")
    Log.Info(f"  [{len(task.data_libraries)}] data libraries")
    Log.Info(f"  [{len(task.transform_libraries)}] transform libraries")
    Log.Info(f"  [{len(task.plan.steps)}] total steps")

    work_relative = AgentPaths.STAGED/task._key
    work_dir = AgentPaths.WORK_ROOT/work_relative
    work_internals = work_dir/AgentPaths.INTERNALS
    data_dir = AgentPaths.to_data()
    data_dir.mkdir(parents=True, exist_ok=True)
    work_internals.mkdir(parents=True, exist_ok=True)
    with RemoteShell(AgentPaths.to_local_relay_coms(host=host), timeout=60) as extern_shell:
        extern_root = agent.real_path
        assert extern_root is not None
        extern_work = extern_root/work_relative
        _rel = f"{extern_work}".replace(f"{extern_root}/", "")
        workspace_str = f"{{AGENT_HOME}}/{_rel}"

        if not verify:
            Log.Info(f"skipping verification of external inputs paths")
        else:
            given_paths = [inst.ResolvePath() for inst in task.plan.given]
            given_paths = [p for p in given_paths if not p.is_relative_to(AgentPaths.HOME_ROOT)]
            def batchify(iterable: Iterable, n):
                batch: list[str] = []
                for x in iterable:
                    if len(batch) >= n:
                        yield batch
                        batch = []
                    batch.append(x)
                if len(batch) > 0: yield batch
            cmd = [
                f'[ -e "{p}" ] && echo "{p}"'
                for p in given_paths
            ]
            found = set()
            bs = 100
            batches = list(batchify(cmd, bs))
            for i, _batch in enumerate(batches):
                Log.Info(f"verifying [{(i*bs)+len(_batch)} of {len(cmd)}] external input paths")
                res = extern_shell.Exec(
                    cmd="\n".join(_batch),
                    history=True
                )
                found |= {Path(p) for p in res.out}
            missing_paths = [p for p in given_paths if p not in found]
            if len(missing_paths)>0:
                Log.Error(f"missing [{len(missing_paths)}] given data:")
                for p in missing_paths:
                    Log.Error(f"    {p}")
                Log.Error(f"staging failed, partial progress at [{workspace_str}]")
                return

    Log.Info(f"work [{work_dir}]")
    Log.Info(f"data [{data_dir}]")
    extern_data = extern_root/data_dir.name
    Log.Info(f"external work [{extern_work}]")
    Log.Info(f"external data [{extern_data}]")

    # data libraries
    def move_remote_libs(libs: list[DataInstanceLibrary], dest: Path):
        processed_libs: list[DataInstanceLibrary] = []
        mover = Logistics()
        expected: list[Source] = []
        to_pull = [lib for lib in libs if lib.remote_src is not None]
        if len(to_pull)==0: return libs
        Log.Info(f"pulling [{len(to_pull)}] remote data libraries to [{data_dir}]")
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
    Log.Info(f"compiling nextflow script")
    task.PrepareNextflow(NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=work_dir,
        external_work=extern_work,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=agent.home.GetPath(),
        container_runtime=agent.runtime,
        resources_file=AgentPaths.NXF_RES,
    ))
    nxflib_dir = work_dir/"lib"
    nxflib_dir.mkdir(parents=True, exist_ok=True)
    orchestrator_lib = MODULE_PATH/"nextflow_config/Orchestrator.groovy"
    shutil.copy(orchestrator_lib, nxflib_dir/orchestrator_lib.name)

    # launcher
    launcher_path = work_dir/AgentPaths.LAUNCHER_FILE
    Log.Info(f"creating launcher script at [{launcher_path}]")
    mock = agent._get_mock_container(task)
    binds = mock.MakeBindsParam()
    if len(mock.binds)>0:
        Log.Info(f"external binds {[a for a, b in mock.binds]}")
    with open(launcher_path, "w") as f:
        f.write("\n".join([
            f'#!/bin/bash',
            'cd $( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )',
            "# >>> agent setup commands",
        ]+agent.setup_commands+[
            "# <<<",
            f'TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")',
            f'LOG_DIR="./{AgentPaths.INTERNALS}/logs.$TIMESTAMP"',
            f'LOG_LATEST="./{AgentPaths.INTERNALS}/logs.latest"',
            f'mkdir -p $LOG_DIR',
            f'[ -e $LOG_LATEST ] && rm "$LOG_LATEST"; ln -s "./logs.$TIMESTAMP" "$LOG_LATEST"',
            f'[ -e {AgentPaths.NXF_PARAMS} ] || touch {AgentPaths.NXF_PARAMS}',
            f'[ -e {AgentPaths.NXF_CONFIG} ] || touch {AgentPaths.NXF_CONFIG}',
            f'echo "start time was [$TIMESTAMP]"',
            f'export BINDS="{binds}"',
            f'export OPENBLAS_NUM_THREADS=1',
            f'export OMP_NUM_THREADS=1',
            f'nohup ../../msm api run_workflow -a key={task_key} host=$(hostname) log_dir=$LOG_DIR stub_delay=${{1:-0}} >$LOG_DIR/agent.log 2>&1 &',
        ]))
    os.chmod(launcher_path, 0o754)

    Log.Info(f"drawing DAG")
    task.plan.RenderDAG(f"{work_dir}/workflow.dag.svg")
    Log.Info(f"[{task._key}] staged to [{workspace_str}]")
        
def CollectResults(
    task: WorkflowTask,
    output_path: Path,
    inputs_dir: Path,
    manifests_path: Path,
) -> DataInstanceLibrary:
    """Compile Nextflow outputs into a DataInstanceLibrary with lineage.

    Reads input manifests and output manifests produced by the Orchestrator,
    reconstructs parent-child relationships, and returns the result library.

    Args:
        task: The workflow task that was executed.
        output_path: Path to the results directory (where outputs live).
        inputs_dir: Path to the inputs/ directory with input CSVs.
        manifests_path: Path to the _manifests/ directory with JSON manifests.

    Returns:
        DataInstanceLibrary with all outputs and their lineage.
    """
    output = DataInstanceLibrary(output_path)
    tlibs: dict[str, DataTypeLibrary] = {}
    path2inst: dict[Path, DataInstance] = {}
    for lib in task.transform_libraries:
        for namespace, tlib in lib.types.items():
            tlibs[namespace] = tlib
    for lib in task.data_libraries:
        for namespace, tlib in lib.types.items():
            if namespace in tlibs:
                _lib = tlibs[namespace]
                for k, e in tlib.types.items():
                    if k in _lib: continue
                    _lib[k] = e
            else:
                _lib = tlib
            tlibs[namespace] = _lib
        for path, name, model in lib.Iterate():
            inst = lib.Get(path)
            path2inst[inst.ResolvePath()] = inst
    for namespace, tlib in tlibs.items():
        output.AddTypeLibrary(namespace=namespace, lib=tlib)
    k2inst: dict[str, DataInstance] = {}
    for inst in task.plan.given:
        k2inst[inst.dtype.key] = inst
    for step in task.plan.steps:
        for insts in step.dependency_map.values():
            for inst in insts:
                k2inst[inst.dtype.key] = inst
    # this is a mappping of the (k, v) assinged by the orchestrator during nextflow
    kv2path: dict[tuple[str, int], tuple[Path, dict]] = {}
    for in_manifest in inputs_dir.iterdir():
        k = in_manifest.name
        with open(in_manifest) as f:
            for l in f:
                p = Path(l[:-1])
                _hash = md5(str(p).encode()).hexdigest()
                _hash = int(_hash[:15], 16) # 15 is important as it allows us to disregard the sign of a long and match with java
                kv2path[(k, _hash)] = p, {}
    for manifest in glob(str(manifests_path/"*")):
        manifest = Path(manifest)
        if manifest.suffix != ".json": continue
        inst_k = manifest.name.split(".")[-2] # TAB+TAB+f"index {{ path 'msm_manifest.{out_name}.{inst.dtype.key}.raw' }}",
        _parsed_entries = []
        with open(manifest) as j:
            entries = json.load(j)
            for lin, path in entries:
                try:
                    path = Path(path)
                    lind: dict = json.loads(lin)
                    kv = inst_k, int(lind[inst_k][0]) # the type+index of the entry itself, so there must only be 1 value
                    kv2path[kv] = path, lind
                    _parsed_entries.append({
                        "instance_key": kv[0],
                        "instance_index": kv[1],
                        "path": str(path.relative_to(output_path)),
                        "lineage": lind,
                    })
                except Exception as e:
                    Log.Error(e)
        with open(manifest, "w") as j:
            json.dump(_parsed_entries, j, indent=2)
    relavent_k = {k for k, v in kv2path}
    given_manifest = []
    todo = dict(enumerate(kv2path.items()))
    while len(todo)>0:
        to_del = []
        for i, ((ck, cv), (path, lineage)) in todo.items():
            cinst = k2inst[ck]
            if path.is_relative_to(output_path): # is output
                parents = []
                ok = True
                for pk, pvs in lineage.items():
                    if pk not in relavent_k: continue
                    if pk == ck: continue
                    for pv in pvs:
                        k = (pk, pv)
                        if k not in kv2path: continue # likely due to a merge between branches
                        ppath, _ = kv2path[k]
                        if ppath not in path2inst:
                            ok = False
                            break
                        _inst = path2inst[ppath]
                        _path = _inst.ResolvePath()
                        parents.append((_path, _inst.dtype_name))
                        if _path in output: continue
                    if not ok: break
                if not ok: continue
                _parents = []
                for _path, _name in parents:
                    if _path not in output.manifest:
                        output.AddItem(path=_path, dtype=_name)
                    _parents.append(_path)
                _path = path.relative_to(output_path)
                _path = output.AddItem(
                    path=_path,
                    dtype=cinst.dtype_name,
                    parents=_parents,
                )
                _inst = output.Get(_path)
                _path = _inst.ResolvePath()
                path2inst[_path] = _inst
            else:
                given_manifest.append((ck, cv, cinst.dtype_name, path))
            to_del.append(i)
        assert len(to_del)>0
        for i in to_del:
            del todo[i]
    output.PruneTypes(save=False)
    output.Save()

    _df = pd.DataFrame(given_manifest, columns="instance_key, instance_index, type_name, path".split(", "))
    _df.to_csv(manifests_path/"given.csv", index=False)
    return output

def RunWorkflow(key: str, log_dir: Path, host: str, stub_delay: float):
    task_path = AgentPaths.to_task(key)
    workspace = task_path.parent.parent
    assert workspace.exists(), f"task workspace not found [{workspace}]"

    task = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])
    start_time = log_dir.name.split(".")[-1]
    Log.Info(f"Metasmith version [{VERSION}]")
    Log.Info(f"running workflow [{task._key}]")
    Log.Info(f"start time was [{start_time}]")

    Log.Info(f"loading agent metadata")
    agent = Agent.Load(AgentPaths.to_definition())
    extern_home = agent.home.GetPath()
    extern_workspace = AgentPaths.to_task(key, root=extern_home).parent.parent
    (workspace/log_dir).mkdir(parents=True, exist_ok=True)
    MAIN_LOG = workspace/log_dir/AgentPaths.MAIN_LOG_FILE # this is the stdout captured by launcher
    Log.AddLogFile(MAIN_LOG)

    Log.Info(f"workspace [{workspace}]")
    Log.Info(f"external workspace [{extern_workspace}]")
    Log.Info(f"workflow steps [{len(task.plan.steps)}]")
    samples = max(len(instances) for step in task.plan.steps for instances in step.dependency_map.values())
    Log.Info(f"samples estimate [{samples}]")

    if agent.globus_uuid is not None:
        Log.Info(f"locating input data with agent's globus endpoint [{agent.globus_uuid}]")
        dest_base = GlobusSource(endpoint=agent.globus_uuid, path=Path("/")).AsSource()
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
            lib.ActualizeRemote(extern_dest=dest, label=f"msm_staging.{_name}")

    # need to call nf inside container
    # nf needs java and is not a standalone executable
    #
    # https://github.com/nextflow-io/nextflow/discussions/4711
    # export NXF_ENABLE_VIRTUAL_THREADS=false
    # https://seqera.io/blog/optimizing-nextflow-for-hpc-and-cloud-at-scale/
    # export NXF_JVM_ARGS="-Xms2g -Xmx64g"
    results_folder = "results"
    nxf_report = log_dir/"nxf_report.html"
    nxf_dag = log_dir/"workflow.dag_nxf.dot"
    output_path = workspace/results_folder
    if output_path.exists(): shutil.rmtree(output_path)
    manifests_path = output_path/"_manifests"
    manifests_path.mkdir(parents=True, exist_ok=True)
    with LiveShell() as shell:
        shell.RegisterOnOut(Log.Info)
        shell.RegisterOnErr(Log.Error)
        Log.Info(f"calling nextflow from container")
        # export NXF_JVM_ARGS="-Xms16g -Xmx64g"
        # -dump-hashes \
        stub_param = f"-stub --testSpread={stub_delay:0.3f}" if stub_delay>0 else ""
        shell.Exec(
            f"""
            cd {workspace}
            PIDF=./PID.lock
            stop() {{
                [[ -e "$PIDF" ]] && rm $PIDF
                [ -e squeue.log ] && mv squeue.log {log_dir}
                [ -e scancel.log ] && mv scancel.log {log_dir}
                [ -e {AgentPaths.NXF_WORKFLOW} ] && cp {AgentPaths.NXF_WORKFLOW} {log_dir}
                [ -e {AgentPaths.NXF_CONFIG} ] && cp {AgentPaths.NXF_CONFIG} {log_dir}
                [ -e {AgentPaths.NXF_RES} ] && cp {AgentPaths.NXF_RES} {log_dir}
                [ -e {AgentPaths.NXF_PARAMS} ] && cp {AgentPaths.NXF_PARAMS} {log_dir}
                if [ -e {nxf_dag} ]; then
                    dot -Tsvg {nxf_dag} -o {nxf_dag.stem}.svg
                    rm {nxf_dag}
                fi
                exit 0
            }}
            trap stop EXIT

            export NXF_HOME=./.nextflow
            export NXF_ENABLE_VIRTUAL_THREADS=true
            export NXF_OFFLINE=TRUE # don't go online and search for latest version
            export OPENBLAS_NUM_THREADS=1
            export OMP_NUM_THREADS=1
            export NXF_OPTS="-Xms2g -Xmx10g -XX:ActiveProcessorCount=1 -Djdk.virtualThreadScheduler.maxPoolSize=512"
            nextflow \
                -config ./{AgentPaths.NXF_RES} \
                -config ./{AgentPaths.NXF_CONFIG} \
                -log {log_dir}/nxf.log \
                run ./{AgentPaths.NXF_WORKFLOW} \
                -params-file ./{AgentPaths.NXF_PARAMS} \
                --hostName "{host}" \
                --output "{results_folder}" \
                -with-report {nxf_report} \
                -with-dag {nxf_dag} \
                -with-timeline {log_dir}/nxf_timeline.html \
                -with-trace {log_dir}/nxf_trace.tsv \
                {stub_param} \
                -lib ./lib \
                -ansi-log false \
                -resume \
                -work-dir ./nxf_work &
            PID=$!
            echo "nextflow PID is [$PID]"
            echo $PID >$PIDF
            while true; do
                if ! [[ -d "/proc/$PID" ]]; then
                    break
                fi
                if ! [[ -e "$PIDF" ]]; then
                    kill $PID
                    wait $PID
                    break
                fi
                sleep 1
            done
            """,
            timeout=None,
        )

    nxf_report = workspace/nxf_report
    if nxf_report.exists():
        raw_task_meta = None
        with open(nxf_report) as f:
            found = False
            for l in f:
                if l.strip().startswith('window.data = { "trace":['): 
                    found = True
                    continue
                if not found: 
                    continue
                raw_task_meta = json.loads('{ "trace":[' + l[:-2]).get("trace")
                break
        if raw_task_meta is not None:
            try:
                df_tasks = pd.DataFrame(raw_task_meta)
                nxf_task_meta = workspace/log_dir/"nxf_tasks.csv"
                df_tasks.to_csv(nxf_task_meta, index=False)
                Log.Info(f"extracting task metadata to [{nxf_task_meta}]")
            except Exception as e:
                Log.Error(f"failed to parse task metadata from [{nxf_report}] [{e}]")
        else:
            Log.Warn(f"failed to find task metadata table within [{nxf_report}]")
    else:
        Log.Warn(f"no report at [{nxf_report}]")

    Log.Info(f"compiling results")
    output = CollectResults(
        task=task,
        output_path=output_path,
        inputs_dir=output_path.parent/"inputs",
        manifests_path=manifests_path,
    )
    n_outputs = sum(1 for p in output.manifest if Path(p).is_relative_to(output_path) or not Path(p).is_absolute())
    tail = output_path.relative_to(AgentPaths.HOME_ROOT)
    extern_output_path = extern_workspace/results_folder
    external_results_path = extern_home/tail
    Log.Info(f"[{n_outputs}] outputs for [{key}] at [{external_results_path}]")

    Log.Info(f"gathering log files")
    nxf_ids = set()
    nxf_id_len = 9 # 2 + "/" + 6
    # careful, we are also logging to here, so printing may cause infinite loop
    # as new lines are generated
    with open(MAIN_LOG, "r") as f:
        for l in f:
            candidates = re.findall(r"\[[\dabcdef]{2}/[\dabcdef]{6}\]", l)
            if len(candidates) == 0: continue
            hit = candidates[0]
            nxf_id = hit[1:-1] # remove the brackets
            nxf_ids.add(nxf_id)
    NXF_WORK = workspace/"nxf_work"
    PROCESS_DEST = workspace/log_dir/"steps"
    PROCESS_DEST.mkdir(parents=True, exist_ok=True)
    for p in NXF_WORK.glob("*/*"):
        p = p.relative_to(NXF_WORK)
        nxf_id = str(p)[:nxf_id_len]
        if nxf_id not in nxf_ids: continue
        log_path = NXF_WORK/p/".command.log"
        if not log_path.exists(): continue
        if log_path.is_symlink(): continue
        try:
            with open(log_path) as f:
                first_line = f.readline()
                if not re.match(r"step\s?\d+", first_line): continue
                step = [int(x) for x in re.findall(r"\d+", first_line)][0]
                transform = task.plan.steps[step-1].transform
            dest = PROCESS_DEST/f"p{step:02}__{transform.name}_{nxf_id.replace('/', '-')}.log"
            src = log_path
            dest.symlink_to(f"../../../{src.relative_to(workspace)}")
        except:
            continue # if anything happens, abandon hope

    Log.Info(f"linking logs [{log_dir}] to results folder [{output_path}]")
    output_metadata_path = output_path/f"{output._path_to_meta}"
    (output_metadata_path/f"{log_dir.name}").symlink_to(f"../../{log_dir}")
    latest_link = (output_metadata_path/f"logs.latest")
    if latest_link.exists(): latest_link.unlink()
    latest_link.symlink_to(f"../../{log_dir}")
    Log.Info(f"run completed at [{StdTime.Timestamp()}]")

def CheckWorkflow(key: str, index: int|None=None):
    task_path = AgentPaths.to_task(key)
    workspace = task_path.parent.parent
    assert workspace.exists(), f"task workspace not found [{workspace}], maybe it wasn't staged yet"

    Log.Info(f"searching for logs")
    internals = workspace/AgentPaths.INTERNALS
    log_scan_result = list((internals).glob("logs.*"))
    log_dirs = [p for p in log_scan_result if "latest" not in p.name]
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
    
    with open(workspace/log_dir/AgentPaths.MAIN_LOG_FILE, "r") as f:
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
