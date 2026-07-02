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
from collections import deque
from hashlib import md5
import pandas as pd

from .serialization import StdTime
from .hashing import KeyGenerator
from .logging import Log
from .coms.containers import Container, ContainerRuntime
from .coms.terminals import LiveShell, ShellResult, RemoveLeadingIndent
from .coms.via_file_watcher import RemoteShell
from .models.remote import GlobusSource, Logistics, Source, SourceType, SshSource
from .models.workflow import METADATA_FILE, WorkflowStep, WorkflowPlan, WorkflowTarget, WorkflowTask, NextflowGenContext, BIND_FILE
from .models.lineage import LinPayload
from .models.libraries import DataInstanceLibrary, DataInstance, DataTypeLibrary, TransformInstanceLibrary, TransformInstanceLibraryView, DataInstanceLibraryView
from .models.libraries import TransformInstance, Resources
from .models.paths import PathMap
from .models.solver import Dependency, Endpoint, Solution, Transform
from .constants import VERSION, CONTAINER_TAG, MODULE_PATH, AgentPaths

class AgentShell:
    def __init__(self, agent: Agent):
        self.agent = agent
        self.shell: LiveShell | None = None

    def __enter__(self):
        shell = LiveShell()
        try:
            def _on_out(x: str):
                Log.Info(f"> {x}\x1b[0;m", timestamp=False) # to escape nextflow colours
            def _on_err(x: str):
                Log.Error(f"> {x}", timestamp=False)
            Log.Info(f"connecting to deployed agent")
            self.agent._run_setup(shell)
            shell.RegisterOnOut(_on_out)
            shell.RegisterOnErr(_on_err)
            shell.Exec(f"cd {self.agent.home.GetPath()}")
            res = shell.Exec('[ -e ./relay/msm_relay ] && echo "relay-present"', history=True)
            assert "relay-present" in res.out, (
                f"relay binary not present at [{self.agent.home.GetPath()}/relay/msm_relay]; "
                f"agent home may be partially deployed — rerun Agent.Deploy()"
            )
            Log.Info(f"starting relay service")
            shell.Exec(f'./relay/msm_relay start')
            self.shell = shell
            return self.shell
        except BaseException:
            shell.Dispose()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.shell is None: return
        Log.Info(f"closing connection")
        self.agent._run_cleanup(self.shell)
        if self.agent._is_ssh():
            try:
                self.shell.Exec("exit", timeout=5)
            except (KeyboardInterrupt, TimeoutError):
                pass
        self.shell.__exit__(exc_type, exc_val, exc_tb)
        self.shell = None

@dataclass(frozen=True)
class TargetSpec:
    dtype_name: str
    parents: tuple["TargetSpec", ...] = ()

class TargetBuilder:
    def __init__(self) -> None:
        self._items: list[TargetSpec] = []

    def Add(self, target_type: str, parents: Iterable[TargetSpec]|None=None) -> TargetSpec:
        assert "::" in target_type, f'expected @type to in the form of "namespace::type_name" but got [{target_type}]'
        spec = TargetSpec(target_type, tuple(parents or ()))
        for existing in self._items:
            assert existing != spec, f'target [{target_type}] with identical parents already added'
        self._items.append(spec)
        return spec

    def resolve(self) -> list[TargetSpec]:
        # Insertion order is causal: a parent must have been Add'd before its child,
        # since the child receives the parent's TargetSpec handle.
        return list(self._items)

    def __len__(self) -> int:
        return len(self._items)

ResourceOverrides = dict[int|Literal["all"]|Literal["*"]|str|TransformInstance, Resources]
@dataclass
class Agent:
    home: Source
    setup_commands: list[str] = field(default_factory=list)
    container: str = f"docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}"
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
            shell.Exec(f"ssh {ssh_src.host}", inherit_stdin=True)
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
            _quiet = False
            shell.RegisterOnOut(lambda x: (Log.Info(x) if not _quiet else None))
            shell.RegisterOnErr(lambda x: (Log.Error(x) if not _quiet else None))

            def do_step(cmd: str, display_cmd: str|None=None, timeout:float|None=None):
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

            _quiet = True
            self._run_setup(shell)
            _quiet = False

            shell.Exec(f'mkdir -p "{self.home.GetPath()}"')
            _quiet = True
            cmds = [
                f'realpath {self.home.GetPath()}',
                f'realpath ~',
                f'hostname',
            ]
            res = shell.Exec('\n'.join(cmds), history=True)
            _quiet = False
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
                    ("$AGENT_HOME", Path(str(resolved_agent_home))),
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
                )

                # Decide per-host whether to deliver the rootfs as SIF or as
                # an unpacked sandbox dir. The probe is a static two-axis
                # check (setuid starter-suid + apptainer major.minor); SIF is
                # preferred when safe (no disk doubling). Sandbox is built
                # only on apptainer >=1.4 without setuid — the case where SIF
                # engages squashfuse_ll (Bug E.2 wedge under msm_relay on
                # WSL2) and the sandbox path goes through kernel overlayfs.
                # On apptainer 1.3.x without setuid the sandbox path itself
                # falls back to fuse-overlayfs (Bug E.4 SIGBUS on fir under
                # SLURM array contention), so we keep SIF there too. Verdict
                # is re-evaluated on every Deploy(); a stale sandbox from a
                # prior host config is removed when the verdict flips.
                _sandbox_path = container.GetSandboxPath()
                _probe = container.MakeSandboxDecisionProbe()
                _build_sandbox = container.MakeBuildSandboxCommand()
                _force = f'rm -rf {_sandbox_path} && ' if assertive else ''
                do_step(
                    cmd=(
                        f'{_force}'
                        f'VERDICT=$({_probe}); '
                        f'if [ "$VERDICT" = "use-sandbox" ]; then '
                        f'[ -d {_sandbox_path} ] || {_build_sandbox}; '
                        f'else rm -rf {_sandbox_path}; fi'
                    ),
                    display_cmd=f"{{probe host; build sandbox iff apptainer>=1.4 and no setuid}}: apptainer build --sandbox {_sandbox_path.name} {_local_path.name}".replace(str(resolved_agent_home), '$AGENT_HOME'),
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
            # Container extraction is the one truly expensive step left;
            # everything else above is either a no-op (rsync -au on unchanged
            # files) or self-gated ([ -e {sif} ] for the container pull).
            # Skip extraction only when its actual output already exists, so
            # a partial deploy (sif present, relay missing) self-heals on the
            # next call without needing assertive=True.
            relay_bin = AgentPaths.to_relay(self.home.GetPath())
            res = shell.Exec(f'[[ -e "{relay_bin}" ]] && echo "relay-present"', history=True)
            if "relay-present" in res.out and not assertive:
                Log.Info(f"relay binary present at [{relay_bin}], skipping container extraction")
            else:
                do_step(f"{resolved_agent_home}/msm api deploy_from_container -a workspace={AgentPaths.HOME_ROOT} architecture=$(uname -m) system=$(uname -s)")
                res = shell.Exec(f'[[ -e "{relay_bin}" ]] && echo "relay-deployed"', history=True)
                assert "relay-deployed" in res.out, f"deploy_from_container completed but relay binary missing at [{relay_bin}]"
            self._run_cleanup(shell)
            Log.Info(f"deployed to [{self.home.address}]")

    def GenerateWorkflow(
        self,
        samples: Iterable[DataInstanceLibraryView|DataInstanceLibrary],
        resources: Iterable[DataInstanceLibraryView|DataInstanceLibrary],
        transforms: list[TransformInstanceLibrary|TransformInstanceLibraryView],
        targets: TargetBuilder | list[str],
        max_iter: int=1024, max_refine: int=256, seed: int=42,
    ):
        if isinstance(targets, list):
            tb = TargetBuilder()
            for t in targets:
                tb.Add(t)
            targets = tb
        assert len(targets)>0, "[targets] can not be empty"
        
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
        _spec2dep: dict[TargetSpec, Dependency] = {}
        target_names: list[str] = []
        for spec in targets.resolve():
            e = _get_endpoint(spec.dtype_name)
            d = target_model.AddRequirement(example=e, parents={_spec2dep[p] for p in spec.parents})
            _spec2dep[spec] = d
            target_names.append(spec.dtype_name)

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
        orig_resources = [lib if isinstance(lib, DataInstanceLibrary) else lib._original for lib in resources]
        _ok = bool(gen_result.steps) and len(gen_result.dropped_targets) == 0
        return WorkflowTask(ok=_ok, plan=gen_result, data_libraries=list(sample_libs)+orig_resources, transform_libraries=transforms)

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

    def StageWorkflow(self, task: WorkflowTask, on_exist: str = "update", verify_external_paths: bool=False):
        VALID_ON_EXIST = {"skip", "error", "clear", "update", "update_workflow", "update_data"}
        assert on_exist in VALID_ON_EXIST, f"on_exist option [{on_exist}] is not one of {VALID_ON_EXIST}"
        Log.Info(f"staging workflow [{task.GetKey()}]")
        agent_shell = AgentShell(self)
        task_stage_partial = False
        with agent_shell as sh_remote:
            remote_path = AgentPaths.to_task(task._key, root=self.home.GetPath())
            remote_work_path = remote_path.parent.parent
            FLAG = "task already staged"
            res = sh_remote.Exec(f'[ -e {remote_work_path} ] && echo "{FLAG}"', history=True, quiet=True)
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
            launcher_path = remote_work_path / AgentPaths.LAUNCHER_FILE
            res = sh_remote.Exec(f'[ -e {launcher_path} ] && echo "launcher-staged"', history=True, quiet=True)
            assert "launcher-staged" in res.out, f"stage_workflow returned but launcher missing at [{launcher_path}]"

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
            res = sh_remote.Exec(f"[ -e {workspace} ] && echo '{FLAG}'", history=True, quiet=True)
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
            launcher = workspace / AgentPaths.LAUNCHER_FILE
            res = sh_remote.Exec(f"[ -e {launcher} ] && echo 'launcher-present'", history=True, quiet=True)
            assert "launcher-present" in res.out, f"launcher missing at [{launcher}]; re-stage the task"
            sh_remote.Exec(f"{launcher} {stub_delay:0.3f}")

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
                res = sh_remote.Exec(f"[ -e {result_path} ] && echo '{FLAG}'", history=True, quiet=True)
                assert FLAG in res.out, f"results not found at [{self.home.ReplacePathWith(result_path).address}]"

        if self.globus_uuid is not None and allow_globus:
            src = GlobusSource(endpoint=self.globus_uuid, path=result_path).AsSource()
        else:
            src = self.home.ReplacePathWith(result_path)
        return src

    # -- lightweight detached-run helpers -----------------------------------

    def _remote_oneshot(self, cmd: str, timeout: int = 30) -> "ShellResult":
        """One-shot exec on the agent host without holding AgentShell open.

        Local home → transient LiveShell with quiet=True.
        SSH home   → direct subprocess `ssh host '<cmd>'`.
        Returns ShellResult with .out and .err populated.
        """
        if self._is_ssh():
            import subprocess
            ssh_src = SshSource.Parse(self.home.address)
            try:
                proc = subprocess.run(
                    ["ssh", "-o", f"ConnectTimeout={min(timeout, 30)}", "-o", "BatchMode=yes", ssh_src.host, cmd],
                    capture_output=True, text=True, timeout=timeout,
                )
                return ShellResult(
                    out=[ln for ln in proc.stdout.splitlines()],
                    err=[ln for ln in proc.stderr.splitlines()],
                )
            except subprocess.TimeoutExpired as exc:
                return ShellResult(out=[], err=[f"ssh timeout after {timeout}s: {exc}"])
        else:
            with LiveShell() as sh:
                res = sh.Exec(cmd, history=True, quiet=True, timeout=timeout)
            return res

    def _task_workspace(self, task_key: str) -> Path:
        """workspace = runs/<key>/ (parent of _metasmith)"""
        return AgentPaths.to_task(task_key, root=self.home.GetPath()).parent.parent

    def _resolve_run_dir(self, task_key: str, run: int | None) -> Path:
        """Resolve runs/<key>/_metasmith/logs.<ts>/ once at call time."""
        workspace = self._task_workspace(task_key)
        internals = workspace / AgentPaths.INTERNALS
        if run is None:
            latest = internals / "logs.latest"
            res = self._remote_oneshot(f"readlink -f {latest}", timeout=15)
            for line in res.out:
                line = line.strip()
                if line.startswith(str(internals)) or "/logs." in line:
                    return Path(line)
            return latest  # fall back to symlink path
        else:
            res = self._remote_oneshot(
                f"ls -1d {internals}/logs.* 2>/dev/null | grep -v latest | sort",
                timeout=15,
            )
            dirs = [Path(ln.strip()) for ln in res.out if ln.strip()]
            assert 1 <= run <= len(dirs), f"run {run} out of range (1..{len(dirs)})"
            return dirs[run - 1]

    def WaitForWorkflow(
        self,
        task: WorkflowTask | str,
        timeout_s: float = 3600.0,
        poll_s: float = 5.0,
        run: int | None = None,
        sentinel: str = "run completed at",
        since_mtime: float | None = None,
    ) -> dict:
        """Block until `sentinel` appears in agent.log of the selected run.

        Returns: {task_key, status, run_dir, elapsed_s, last_log_mtime, tail}
        status ∈ {"completed", "timeout", "missing", "errored"}.
        """
        import time
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        run_dir = self._resolve_run_dir(task_key, run)
        agent_log = run_dir / "agent.log"
        workspace = self._task_workspace(task_key)
        pid_lock = workspace / "PID.lock"

        start = time.monotonic()
        cur_poll = poll_s
        max_poll = 15.0
        last_mtime: float = 0.0

        while True:
            elapsed = time.monotonic() - start
            cmd = (
                f"if [ -e {agent_log} ]; then "
                f"stat -c %Y {agent_log}; "
                f"grep -c '{sentinel}' {agent_log} 2>/dev/null || echo 0; "
                f"else echo MISSING; echo 0; fi; "
                f"[ -e {pid_lock} ] && echo PID_ALIVE || echo PID_GONE"
            )
            res = self._remote_oneshot(cmd, timeout=30)
            lines = [ln.strip() for ln in res.out if ln.strip()]
            mtime_line = lines[0] if lines else ""
            count_line = lines[1] if len(lines) > 1 else "0"
            pid_line = lines[-1] if lines else "PID_GONE"

            log_exists = mtime_line != "MISSING"
            try:
                last_mtime = float(mtime_line) if log_exists else 0.0
            except ValueError:
                last_mtime = 0.0
            try:
                count = int(count_line)
            except ValueError:
                count = 0

            fresh = (since_mtime is None) or (last_mtime > since_mtime)
            if log_exists and count > 0 and fresh:
                tail = self.TailWorkflowLog(task_key, source="agent", lines=20, run=run)
                return {
                    "task_key": task_key,
                    "status": "completed",
                    "run_dir": str(run_dir),
                    "elapsed_s": elapsed,
                    "last_log_mtime": last_mtime,
                    "tail": tail.get("lines", []),
                }
            if log_exists and pid_line == "PID_GONE" and count == 0 and elapsed > 5.0:
                tail = self.TailWorkflowLog(task_key, source="agent", lines=20, run=run)
                return {
                    "task_key": task_key,
                    "status": "errored",
                    "run_dir": str(run_dir),
                    "elapsed_s": elapsed,
                    "last_log_mtime": last_mtime,
                    "tail": tail.get("lines", []),
                }
            if elapsed > timeout_s:
                tail_lines: list[str] = []
                if log_exists:
                    tail = self.TailWorkflowLog(task_key, source="agent", lines=20, run=run)
                    tail_lines = tail.get("lines", [])
                return {
                    "task_key": task_key,
                    "status": "timeout" if log_exists else "missing",
                    "run_dir": str(run_dir),
                    "elapsed_s": elapsed,
                    "last_log_mtime": last_mtime,
                    "tail": tail_lines,
                }
            time.sleep(cur_poll)
            cur_poll = min(max_poll, cur_poll * 1.3)

    def TailWorkflowLog(
        self,
        task: WorkflowTask | str,
        source: str = "agent",
        lines: int = 50,
        run: int | None = None,
    ) -> dict:
        """Read the last N lines from agent.log or main.log of the selected run."""
        assert source in ("agent", "main"), f"source must be 'agent' or 'main', got [{source}]"
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        run_dir = self._resolve_run_dir(task_key, run)
        if source == "agent":
            log_path = run_dir / "agent.log"
        else:
            log_path = run_dir / AgentPaths.MAIN_LOG_FILE
        res = self._remote_oneshot(
            f"[ -e {log_path} ] && tail -n {lines} {log_path} || echo __MSM_MISSING__",
            timeout=30,
        )
        out = res.out
        exists = not (len(out) == 1 and out[0].strip() == "__MSM_MISSING__")
        return {
            "task_key": task_key,
            "source": source,
            "run": run,
            "run_dir": str(run_dir),
            "file": str(log_path),
            "exists": exists,
            "lines": out if exists else [],
        }

    def CancelWorkflow(self, task: WorkflowTask | str, timeout_s: float = 30.0) -> dict:
        """Best-effort cancel an active run by removing workspace/PID.lock.

        The launcher (see RunWorkflow) watches PID.lock and gracefully kills
        nextflow when it disappears. Falls back to pkill if the lock is gone
        but the driver is still alive.
        """
        import time
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        workspace = self._task_workspace(task_key)
        pid_lock = workspace / "PID.lock"

        probe = self._remote_oneshot(f"[ -e {pid_lock} ] && cat {pid_lock} || echo __MSM_NONE__", timeout=15)
        first = probe.out[0].strip() if probe.out else "__MSM_NONE__"
        if first == "__MSM_NONE__":
            return {
                "task_key": task_key,
                "method": "noop",
                "killed_pid": None,
                "status": "not_running",
                "detail": "PID.lock not present",
            }
        try:
            pid = int(first)
        except ValueError:
            pid = None

        self._remote_oneshot(f"rm -f {pid_lock}", timeout=15)

        start = time.monotonic()
        while time.monotonic() - start < timeout_s:
            alive = self._remote_oneshot(
                f"[ -e /proc/{pid} ] && echo ALIVE || echo GONE" if pid else "echo GONE",
                timeout=15,
            )
            if alive.out and alive.out[0].strip() == "GONE":
                return {
                    "task_key": task_key,
                    "method": "pidfile",
                    "killed_pid": pid,
                    "status": "cancelled",
                    "detail": "PID.lock removed; driver exited",
                }
            time.sleep(1.0)

        # fallback
        self._remote_oneshot(f"pkill -f 'run_workflow.*key={task_key}' || true", timeout=15)
        return {
            "task_key": task_key,
            "method": "pkill_fallback",
            "killed_pid": pid,
            "status": "cancelled",
            "detail": "PID.lock removal did not stop driver within timeout; pkill fallback issued",
        }

    def ListWorkflowRuns(self, task: WorkflowTask | str) -> list[dict]:
        """List all runs (logs.<ts> directories) for a task."""
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        internals = self._task_workspace(task_key) / AgentPaths.INTERNALS
        res = self._remote_oneshot(
            f"ls -1d {internals}/logs.* 2>/dev/null | grep -v latest | sort",
            timeout=15,
        )
        runs: list[dict] = []
        for i, line in enumerate(ln.strip() for ln in res.out if ln.strip()):
            ts = line.rsplit(".", 1)[-1] if "." in line else ""
            runs.append({"index": i + 1, "path": line, "timestamp": ts})
        return runs

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

    work_relative = AgentPaths.STAGED/task_key
    work_dir = AgentPaths.WORK_ROOT/work_relative
    work_internals = work_dir/AgentPaths.INTERNALS
    data_dir = AgentPaths.to_data()
    data_dir.mkdir(parents=True, exist_ok=True)
    work_internals.mkdir(parents=True, exist_ok=True)
    with RemoteShell(AgentPaths.to_local_relay_coms(host=host), timeout=60) as extern_shell:
        extern_root = agent.real_path
        assert extern_root is not None
        path_map = PathMap(extern_home=Path(str(extern_root)), task_key=task._key)
        extern_work = path_map.extern_work
        workspace_str = f"{{AGENT_HOME}}/{path_map.extern_work.relative_to(extern_root)}"

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
            f"[ -e {AgentPaths.NXF_PARAMS} ] || echo '{{}}' > {AgentPaths.NXF_PARAMS}",
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
) -> DataInstanceLibrary:
    """Compile Nextflow outputs into a DataInstanceLibrary with lineage.

    Walks `<workspace>/_metasmith/trace.jsonl` for per-batch
    InvocationEvent rows (produced by promote_run / cache-hit emission)
    and reconstructs parent-child relationships from `consumes` /
    `produces`. The legacy `_manifests/*.json` sidecar route is gone
    as of S6.

    Args:
        task: The workflow task that was executed.
        output_path: Path to the results directory (where outputs live).
        inputs_dir: Path to the inputs/ directory with input CSVs.

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
    inst_id2inst: dict[str, DataInstance] = {}
    # path2iid replaces the old input_ids/ sidecar: the given DataInstances are
    # themselves the record of <path> -> <instance_id> (the input CSVs write
    # x.ResolvePath(); this maps the same path back to x.instance_id). Sourcing
    # it from task.plan.given (+ step deps) removes the sidecar's separate copy.
    path2iid: dict[str, str] = {}
    for inst in task.plan.given:
        inst_id2inst[inst.instance_id] = inst
        path2iid[str(inst.ResolvePath())] = inst.instance_id
    for step in task.plan.steps:
        for insts in step.dependency_map.values():
            for inst in insts:
                inst_id2inst[inst.instance_id] = inst
                path2iid[str(inst.ResolvePath())] = inst.instance_id

    def _resolve_instance(dtype_key: str, instance_id: str | None = None):
        """Direct lookup by instance_id (G2). No dtype_key fallback.

        The input-CSV writer at workflow.py emits `<path>\\t<instance_id>`
        rows; manifest filenames carry slot_id in the `inst_id` field.
        Both routes populate `instance_id` end-to-end, so the legacy
        collision fallback (multiple candidates → deterministic first)
        is no longer needed.
        """
        if instance_id is None:
            raise KeyError(
                f"_resolve_instance called without instance_id for [{dtype_key}]; "
                f"input CSV writer should always emit <path>\\t<instance_id>"
            )
        try:
            return inst_id2inst[instance_id]
        except KeyError:
            raise KeyError(
                f"missing DataInstance for instance_id [{instance_id}] (dtype={dtype_key})"
            )
    # Mapping of (dtype_key, hash15(abs_path)) → (path, lineage, slot/csv id, file_instance_id).
    # The fourth element is the trace's ProducedFile.file_instance_id (G3); it's
    # None for input entries (which only carry the csv-routed slot identity) and
    # is the bridge between the trace's per-file identity and the published-results
    # manifest's instance_id. Consumed by SetLineageInstance(...) after AddItem
    # below so `_resolve_instance_meta` on reload no longer falls back to the
    # legacy path+dtype derivation for promoted outputs.
    kv2path: dict[tuple[str, int], tuple[Path, dict, str|None, str|None]] = {}
    # The input CSVs are path-only (Nextflow's Channel.splitCsv consumes them);
    # the instance_id that routes each path to its DataInstance comes from
    # path2iid (built above from the given record), not a separate sidecar file.
    for in_manifest in inputs_dir.iterdir():
        k = in_manifest.name
        with open(in_manifest) as f:
            for l in f:
                p = Path(l[:-1])
                _hash = md5(str(p).encode()).hexdigest()
                _hash = int(_hash[:15], 16) # 15 is important as it allows us to disregard the sign of a long and match with java
                kv2path[(k, _hash)] = p, {}, path2iid.get(str(p)), None
    # C1: BFS over `_metasmith/trace.jsonl` populates the output side of
    # `kv2path` directly from `InvocationEvent.consumes`, replacing the
    # legacy `_manifests/*.json` glob. The trace is authoritative post
    # C0/C0.5: every promote/hit event carries `consumes` (slot-keyed
    # parent ids) and per-file `ProducedFile.path` + `file_instance_id`.
    # publishDir for `_manifests/` is still in place — C2's job to drop.
    from .telemetry import TraceIndex
    trace_idx = TraceIndex.read(output_path.parent / "_metasmith" / "trace.jsonl")
    # trace.jsonl carries slot_ids (assigned by _compute_cache_decisions
    # at PrepareNextflow time); the on-disk task loaded above still
    # holds pre-refresh short instance_ids because PrepareNextflow is
    # not rerun in RunWorkflow. Bridge the two so `_resolve_instance`
    # can map a slot_id back to a DataInstance via inst_id2inst — both
    # ids end up pointing at the same in-memory DataInstance whose
    # dtype_name we ultimately need at `output.AddItem`.
    for _ev in trace_idx.events:
        if not _ev.step_order:
            continue
        _si = _ev.step_order - 1
        if not (0 <= _si < len(task.plan.steps)):
            continue
        _step = task.plan.steps[_si]
        _dtype_to_insts: dict[str, list[DataInstance]] = {}
        for _dep_group in _step.transform.model.produces:
            for _dep in _dep_group:
                for _inst in _step.dependency_map.get(_dep, []):
                    _dtype_to_insts.setdefault(_inst.dtype.key, []).append(_inst)
        for _pf in _ev.produces:
            _cands = _dtype_to_insts.get(_pf.dtype_key, [])
            if _cands:
                if _pf.slot_id:
                    inst_id2inst.setdefault(_pf.slot_id, _cands[0])
                if _pf.file_instance_id:
                    inst_id2inst.setdefault(_pf.file_instance_id, _cands[0])
    # consumes values are `hex(utf8(instance_id))` (workflow.py:1287),
    # while TraceIndex.by_slot / by_file key on the raw instance_id —
    # decode once to bridge. Empty for fixtures with no upstream chain
    # (every input is a leaf given), and `_try_decode` falls back to
    # the raw form for trace shapes that ever emit it directly.
    def _try_decode(piid: str) -> str | None:
        try:
            return bytes.fromhex(piid).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return None
    # Map every produced slot_id / file_instance_id to ALL events that
    # produced it. A slot_id is shared across every batch of a step (the
    # intermediate bam of a 3-sample diamond has one slot_id but three
    # producer events, one per sample), so a single-winner map collapses
    # multi-batch producers to the first event — the root of Bug L/I11: a
    # downstream task that references the bam by slot_id would always walk
    # back into sample 0's alignment. Keep the full producer list and
    # disambiguate per walk-branch by shared given-ancestry (below).
    _slot_to_events: dict[str, list] = {}
    def _add_producer(key, ev):
        if not key:
            return
        lst = _slot_to_events.setdefault(key, [])
        if ev not in lst:
            lst.append(ev)
    for _ev in trace_idx.events:
        for _pf in _ev.produces:
            _add_producer(_pf.slot_id, _ev)
            _add_producer(_pf.file_instance_id, _ev)

    def _event_given_ids(ev) -> set[str]:
        """Given-leaf instance_ids directly referenced by `ev.consumes`.

        A "given" here is a consumes id that resolves to a DataInstance
        but is produced by no event (a true leaf) — that set is the
        event's *sample identity*. The diamond's alignment:i and
        binner:i both reference assembly_i directly, so intersecting
        these sets tells us which bam producer feeds which binner
        without any runtime capture. Bridged slot/file ids (added to
        inst_id2inst above) are produced, so they're excluded.
        """
        out: set[str] = set()
        for _vals in ev.consumes.values():
            for _piid in _vals:
                for _key in (_try_decode(_piid), _piid):
                    if _key and _key in inst_id2inst and _key not in _slot_to_events:
                        out.add(_key)
                        break
        return out

    def _producers_for(piid, root_givens):
        """Producer events for a consumes id, disambiguated by sample.

        When a slot_id names multiple producers (one per batch), keep
        only those sharing a given ancestor with the walk root. If none
        share one — a wildcard/cartesian join, or a root with no direct
        given to key on — fall back to the first producer (legacy
        single-winner behaviour), so already-correct walks don't shift.
        Returns None when the id names no producer (it's a given leaf).
        """
        for _key in (_try_decode(piid), piid):
            if _key and _key in _slot_to_events:
                cands = _slot_to_events[_key]
                if len(cands) <= 1:
                    return list(cands)
                if root_givens:
                    filtered = [e for e in cands if _event_given_ids(e) & root_givens]
                    if filtered:
                        return filtered
                return [cands[0]]
        return None

    def _seed_given_lineage(inst, lind: dict[str, list[int]]) -> None:
        """Walk `parent_lib.parents` transitively from a given DataInstance.

        Mirrors virtual_runtime._seed_lineage (virtual_runtime.py:308):
        the legacy `_manifests/*.json` rows carried this full ancestry
        chain inline, not just direct parents. Reproducing it here keeps
        downstream parent-walk lookups (the `lineage.items()` loop
        below) able to find given-side `(dtype, hash15)` entries from
        kv2path's input-CSV side.
        """
        stack = [inst]
        seen: set[tuple[str, str]] = set()
        while stack:
            curr = stack.pop()
            pl = getattr(curr, "parent_lib", None)
            mark = (pl.GetKey() if pl is not None else "", str(curr.path))
            if mark in seen:
                continue
            seen.add(mark)
            p = curr.ResolvePath()
            lind.setdefault(curr.dtype.key, []).append(
                int(md5(str(p).encode()).hexdigest()[:15], 16)
            )
            if pl is None:
                continue
            for pm in pl.parents.get(curr.path, []):
                if pm.path in pl.manifest:
                    stack.append(pl.Get(pm.path))

    def _build_transitive_lind(root_ev, root_pf):
        """BFS over `consumes`; returns {dtype_key: sorted [hash15(abs_path)]}.

        Reproduces post-hoc what virtual_runtime's `_merge_lineage`
        built at channel-merge time. Each reached event contributes its
        own produces' (dtype_key, hash15(abs_path)) to the accumulator.
        Frontier branches terminating at a given (no producer event)
        seed via `_seed_given_lineage` so the ancestry chain on the
        input side of the library reaches the kv2path lookup table.

        Multi-slot events emit multiple ProducedFile siblings under a
        single event; siblings are co-produced, not each other's
        ancestors. For the ROOT event, include only `root_pf` (the file
        whose lineage is being computed) so the downstream resolver in
        CollectResults does not treat sibling outputs as parents. All
        ancestor events still contribute their full produces.
        """
        lind: dict[str, list[int]] = {}
        if root_pf.path and root_pf.dtype_key:
            rel = Path(root_pf.path)
            abs_p = output_path / rel if not rel.is_absolute() else rel
            lind.setdefault(root_pf.dtype_key, []).append(
                int(md5(str(abs_p).encode()).hexdigest()[:15], 16)
            )
        # The walk root's sample identity: the given leaves its producing
        # event directly consumes. Every ancestor of root_pf shares it,
        # so it disambiguates multi-producer slot references (Bug L/I11).
        root_givens = _event_given_ids(root_ev)
        seen_evs: set[str] = {root_ev.task_hash}
        frontier: list = []
        # Seed frontier from root_ev's consumes (without re-adding its
        # own produces — those are the file we're computing lineage FOR
        # plus its siblings).
        for parent_iids in root_ev.consumes.values():
            for piid in parent_iids:
                parent_evs = _producers_for(piid, root_givens)
                if parent_evs:
                    frontier.extend(parent_evs)
                    continue
                given = None
                for key in (_try_decode(piid), piid):
                    if key and key in inst_id2inst:
                        given = inst_id2inst[key]
                        break
                if given is not None:
                    _seed_given_lineage(given, lind)
        while frontier:
            nxt = []
            for ev in frontier:
                if ev.task_hash in seen_evs:
                    continue
                seen_evs.add(ev.task_hash)
                for pf in ev.produces:
                    if not pf.path or not pf.dtype_key:
                        continue
                    rel = Path(pf.path)
                    abs_p = output_path / rel if not rel.is_absolute() else rel
                    lind.setdefault(pf.dtype_key, []).append(
                        int(md5(str(abs_p).encode()).hexdigest()[:15], 16)
                    )
                for parent_iids in ev.consumes.values():
                    for piid in parent_iids:
                        parent_evs = _producers_for(piid, root_givens)
                        if parent_evs:
                            for pe in parent_evs:
                                if pe.task_hash not in seen_evs:
                                    nxt.append(pe)
                            continue
                        given = None
                        for key in (_try_decode(piid), piid):
                            if key and key in inst_id2inst:
                                given = inst_id2inst[key]
                                break
                        if given is not None:
                            _seed_given_lineage(given, lind)
            frontier = nxt
        return {k: sorted(set(v)) for k, v in lind.items()}

    for ev in trace_idx.events:
        for pf in ev.produces:
            if not pf.path or not pf.dtype_key:
                continue
            rel_path = Path(pf.path)
            abs_path = output_path / rel_path if not rel_path.is_absolute() else rel_path
            _hash = int(md5(str(abs_path).encode()).hexdigest()[:15], 16)
            kv = pf.dtype_key, _hash
            try:
                lind = _build_transitive_lind(ev, pf)
            except Exception as e:
                Log.Error(e)
                continue
            # Preserve CSV-side instance_id if a prior input entry
            # already claimed this kv (G2 routing identity); otherwise
            # fall back to the slot_id from the trace event.
            prior = kv2path.get(kv)
            csv_inst_id = prior[2] if prior else None
            kv2path[kv] = (
                abs_path,
                lind,
                (csv_inst_id or pf.slot_id or None),
                pf.file_instance_id,
            )
    relavent_k = {k for k, v in kv2path}
    given_manifest = []
    todo = dict(enumerate(kv2path.items()))
    prev_len = len(todo) + 1
    while len(todo)>0:
        if len(todo) == prev_len:
            for i, ((ck, cv), (path, lineage, cinst_id, file_inst_id)) in todo.items():
                Log.Warn(f"dropping entry with unresolvable lineage: [{ck}] path=[{path}]")
            break
        prev_len = len(todo)
        to_del = []
        for i, ((ck, cv), (path, lineage, cinst_id, file_inst_id)) in todo.items():
            cinst = _resolve_instance(ck, cinst_id)
            if path.is_relative_to(output_path): # is output
                parents = []
                ok = True
                for pk, pvs in lineage.items():
                    if pk not in relavent_k: continue
                    if pk == ck: continue
                    for pv in pvs:
                        k = (pk, pv)
                        if k not in kv2path: continue # likely due to a merge between branches
                        ppath, _, _, _ = kv2path[k]
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
                # G3: persist the trace's file_instance_id into the published
                # manifest so DataInstanceLibrary.Load on a downstream consumer
                # sees `instance_id == ProducedFile.file_instance_id` instead of
                # the legacy (path + dtype + lib_key) fallback. Bridges
                # walk_ancestors / get_lineage_of into the trace index.
                if file_inst_id is not None:
                    output.SetLineageInstance(
                        path=_path,
                        instance_id=file_inst_id,
                        lineage_payload=b"",
                        origin="lineage",
                    )
                _inst = output.Get(_path)
                _path = _inst.ResolvePath()
                path2inst[_path] = _inst
            else:
                given_manifest.append((cinst.instance_id, cinst.dtype.key, str(path), cinst.origin))
            to_del.append(i)
        assert len(to_del)>0
        for i in to_del:
            del todo[i]
    output.PruneTypes(save=False)
    output.Save()

    _df = pd.DataFrame(given_manifest, columns=["instance_id", "dtype_key", "path", "origin"])
    _df.to_csv(output_path/"given.csv", index=False)
    return output

def _extract_nxf_task_metadata(log_dir_abs: Path) -> "pd.DataFrame | None":
    """Return the per-task Nextflow trace table, or None if unavailable.

    Prefers `nxf_trace.tsv` (produced via `-with-trace`): a clean TSV
    with no escape ambiguity. Falls back to scraping `nxf_report.html`
    if the TSV is missing, sanitizing JS-only escapes (`\\'`) that
    strict JSON rejects -- see inbox #162.
    """
    tsv = log_dir_abs/"nxf_trace.tsv"
    if tsv.exists():
        try:
            return pd.read_csv(tsv, sep="\t")
        except Exception as e:
            Log.Warn(f"failed to read [{tsv}] [{e}], falling back to HTML report")

    html = log_dir_abs/"nxf_report.html"
    if not html.exists():
        return None
    try:
        raw = None
        with open(html) as f:
            found = False
            for l in f:
                if l.strip().startswith('window.data = { "trace":['):
                    found = True
                    continue
                if not found:
                    continue
                # Nextflow embeds the trace as a JS object literal.
                # Single quotes in `.command.sh` arrive here as `\'`,
                # which json.loads rejects. Stripping the backslash
                # yields a valid JSON string (single quotes don't need
                # escaping in JSON).
                sanitized = l[:-2].replace("\\'", "'")
                raw = json.loads('{ "trace":[' + sanitized).get("trace")
                break
        if raw is None:
            return None
        return pd.DataFrame(raw)
    except Exception as e:
        Log.Warn(f"failed to parse task metadata from [{html}] [{e}]")
        return None


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
    path_map = PathMap(extern_home=Path(str(extern_home)), task_key=key)
    extern_workspace = path_map.extern_work
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
            _extern_location = path_map.LocalToExternal(lib.location)
            Log.Info(f"[{_name}] at [{lib.location}] is remote [{lib.remote_src.address}], downloading to [{_extern_location}]")
            dest = dest_base/str(_extern_location)
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
    output_path.mkdir(parents=True, exist_ok=True)
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
                -work-dir {workspace}/nxf_work &
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

    df_tasks = _extract_nxf_task_metadata(workspace/log_dir)
    if df_tasks is not None:
        nxf_task_meta = workspace/log_dir/"nxf_tasks.csv"
        df_tasks.to_csv(nxf_task_meta, index=False)
        Log.Info(f"extracted task metadata to [{nxf_task_meta}]")
    else:
        Log.Warn(f"no task metadata extracted from [{workspace/log_dir}]")

    # S5 — post-execution promote. Walks workflow.step_*.meta, locates
    # each step's outputs, deposits them in the cache, and inserts into
    # CacheStore. Skipped when METASMITH_CACHE is falsy (kill-switch).
    if os.environ.get("METASMITH_CACHE", "1").lower() not in {
        "0", "false", "off", "no"
    }:
        try:
            from .caching.promote import promote_run

            agent_home = Path(str(extern_home))
            cache_root = agent_home / "task_cache"
            summary = promote_run(workspace=workspace, cache_root=cache_root)
            if summary.get("promoted") or summary.get("skipped"):
                Log.Info(
                    "cache promote: "
                    f"{len(summary['promoted'])} written, "
                    f"{len(summary['skipped'])} skipped"
                )
        except Exception as e:
            Log.Warn(f"cache promote failed: {e}")

    Log.Info(f"compiling results")
    output = CollectResults(
        task=task,
        output_path=output_path,
        inputs_dir=output_path.parent/"inputs",
    )
    n_outputs = sum(1 for p in output.manifest if Path(p).is_relative_to(output_path) or not Path(p).is_absolute())
    extern_output_path = extern_workspace/results_folder
    external_results_path = path_map.LocalToExternal(output_path)
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

def CheckWorkflow(key: str, index: int|None=None, quiet: bool=False) -> dict:
    task_path = AgentPaths.to_task(key)
    workspace = task_path.parent.parent
    assert workspace.exists(), f"task workspace not found [{workspace}], maybe it wasn't staged yet"

    internals = workspace/AgentPaths.INTERNALS
    log_scan_result = list((internals).glob("logs.*"))
    log_dirs = [p for p in log_scan_result if "latest" not in p.name]
    log_dirs = sorted(log_dirs, key=lambda x: x.name)

    result = {
        "key": key,
        "runs": [{"index": i+1, "name": d.name, "path": str(d)} for i, d in enumerate(log_dirs)],
        "total_runs": len(log_dirs),
        "selected_run": None,
        "log_content": None,
    }

    if len(log_dirs) == 0:
        if not quiet:
            Log.Warn(f"no logs found for [{key}]")
        return result

    if not quiet:
        Log.Info(f"found [{len(log_dirs)}] runs")
        for i, log_entry in enumerate(log_dirs):
            n_str = f"{i+1}"
            Log.Info(f"{' '*(5-len(n_str))}{n_str}: [{log_entry.name}]")

    log_dir = log_dirs[-1]
    selected_index = len(log_dirs)
    msg = f"here is the main log of the latest run [{log_dir.name}]"
    if index is not None:
        if index < 1 or index > len(log_dirs):
            if not quiet:
                Log.Warn(f"index [{index}] out of range")
        else:
            log_dir = log_dirs[index-1]
            selected_index = index
            msg = f"here is the main log for run [{index}] [{log_dir.name}]"

    result["selected_run"] = {"index": selected_index, "name": log_dir.name, "path": str(workspace/log_dir)}
    with open(workspace/log_dir/AgentPaths.MAIN_LOG_FILE, "r") as f:
        result["log_content"] = f.read()

    if not quiet:
        Log.Info(msg)
        Log.Info(f">"*len(msg))
        Log.Info("")
        lines = result["log_content"].splitlines(keepends=True)
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

    return result
