"""The verbs a client calls on an agent: generate, stage, run, check.

A mixin rather than a module of functions, because unlike the workflow package's
codegen these read heavily off the agent -- its home, its environment, its shell
-- and rewriting them as free functions taking `agent` would be a rename, not a
move. `Agent` inherits it; nothing else should.

Three of these share a name with a free function in `runner`. That is not an
accident of one namespace: `Agent.RunWorkflow` is the client side asking, and
`runner.RunWorkflow` is the agent side doing. Both are public entry points, so
neither gets renamed -- the split is what makes the pair legible.
"""

from __future__ import annotations

import re
import shutil
import tempfile
from pathlib import Path
from typing import Iterable

import yaml

from ..constants import AgentPaths, MODULE_PATH
from ..env import ContainerDef, Environment
from ..logging import Log
from ..models.libraries import (
    DataInstanceLibrary, DataInstanceLibraryView, Gpu, Resources,
    TransformInstanceLibrary, TransformInstanceLibraryView,
)
from ..models.remote import GlobusSource, Logistics, Source
from ..models.solver import Dependency, Transform
from ..models.workflow import WorkflowPlan, WorkflowTask
from ..coms.terminals import IDLE_TIMEOUT, PROBE_TIMEOUT
from .gpu import _plan_gpu_requests, _read_gpu_manifest, _render_gpu_config
from .portability import _check_env_portability, _read_env_manifest
from .shell import AgentShell
from .spec import Spec
from .targets import ResourceOverrides, TargetBuilder, TargetSpec


def GetNxfConfigPresets(folder: Path = MODULE_PATH/"nextflow_config") -> dict[str, Path]:
    """The nextflow configs that ship with metasmith, by name.

    A package folder, so the list is the same for every agent: which one an
    agent uses is a per-agent choice, but the options are not. Module-level so
    that a caller who only wants to know the names -- validating a
    `default_preset` before it is written -- does not need an Agent to ask.
    """
    if not folder.exists(): raise FileNotFoundError(folder)
    presets: dict[str, Path] = {}
    for f in folder.iterdir():
        if f.is_dir(): continue
        if not f.name.endswith(".nf"): continue
        presets[f.stem] = f.absolute()
    return presets


class _WorkflowOps:
    def GenerateWorkflow(
        self,
        samples: Iterable[DataInstanceLibraryView|DataInstanceLibrary],
        resources: Iterable[DataInstanceLibraryView|DataInstanceLibrary],
        transforms: list[TransformInstanceLibrary|TransformInstanceLibraryView],
        targets: TargetBuilder | list[str],
        max_iter: int=256, max_refine: int=256, seed: int=42,
    ):
        """Solve, from libraries already in hand.

        A method on `Agent` because that is how every notebook spells it, but it
        reads nothing off the agent -- planning happens here, and only the
        result is ever sent anywhere. The body is `Spec.SolveViews`, which the
        web GUI and the CLI reach through `Spec.Solve`.
        """
        return Spec.SolveViews(
            samples=samples, resources=resources, transforms=transforms,
            targets=targets, max_iter=max_iter, max_refine=max_refine, seed=seed,
        )

    def _get_mock_container(self, task: WorkflowTask):
        binds = task.GetCommonInputFolders(method="external")
        mock = Environment(
            image=self.container,
            runtime=self.runtime,
            native=self.native,
            container=ContainerDef(binds=[
                (p, p)
                for p in binds
            ]),
        )
        return mock

    def StageWorkflow(
        self, task: WorkflowTask, on_exist: str = "update",
        verify_external_paths: bool=False, idle_timeout: float|None = IDLE_TIMEOUT,
    ):
        """`idle_timeout` bounds each agent-side step by how long it may say
        nothing; None restores the old unbounded wait. The file transfers inside
        SaveAs are bounded too, but by the module default rather than by this
        argument -- override METASMITH_IDLE_TIMEOUT to move both together."""
        task.RefuseIfDeferred()
        VALID_ON_EXIST = {"skip", "error", "clear", "update", "update_workflow", "update_data"}
        assert on_exist in VALID_ON_EXIST, f"on_exist option [{on_exist}] is not one of {VALID_ON_EXIST}"
        Log.Info(f"staging workflow [{task.GetKey()}]")
        agent_shell = AgentShell(self)
        task_stage_partial = False
        with agent_shell as sh_remote:
            remote_path = AgentPaths.to_task(task._key, root=self.home.GetPath())
            remote_work_path = remote_path.parent.parent
            FLAG = "task already staged"
            res = sh_remote.Exec(
                f'[ -e {remote_work_path} ] && echo "{FLAG}"', history=True, quiet=True,
                idle_timeout=PROBE_TIMEOUT, what="checking whether the task is already staged",
            )
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
                        sh_remote.Exec(
                            f"mv {_to_delete_src} {_to_delete} && rm -rf {_to_delete}",
                            idle_timeout=idle_timeout, what="clearing the previously staged task",
                        )
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
            # Pre-flight: external (absolute-path) input folders are bound
            # verbatim into the remote container -- metasmith does NOT transfer
            # them. If a bound source is absent on the remote, the runtime
            # aborts container creation and the launcher is never written, which
            # surfaces much later as an opaque "launcher missing" assertion.
            # Catch it here and name the input that caused it. [#240]
            if len(mock.container.binds) > 0:
                _srcs = [str(src) for src, _dst in mock.container.binds]
                _check = "\n".join(f'[ -e "{s}" ] || echo "MISSING::{s}"' for s in _srcs)
                _res = sh_remote.Exec(_check, history=True, quiet=True)
                _out = _res.out if isinstance(_res.out, str) else "\n".join(_res.out)
                _missing = [ln.split("MISSING::", 1)[1].strip()
                            for ln in _out.splitlines() if "MISSING::" in ln]
                if _missing:
                    _culprits: dict[str, list[str]] = {}
                    for _inst in task.plan.given:
                        try:
                            _p = _inst.ResolvePath()
                        except Exception:
                            continue
                        if not _p.is_absolute():
                            continue
                        for _m in _missing:
                            _mp = Path(_m)
                            if _p == _mp or _p.is_relative_to(_mp):
                                _culprits.setdefault(_m, []).append(str(_p))
                    _lines = "\n".join(
                        f"  - {_m}" + (f"  (from input: {', '.join(_culprits[_m])})"
                                       if _culprits.get(_m) else "")
                        for _m in _missing
                    )
                    raise FileNotFoundError(
                        f"cannot stage workflow [{task._key}]: {len(_missing)} external "
                        f"input folder(s) must be bound into the remote container but do "
                        f"not exist on the remote host:\n{_lines}\n"
                        f"External (absolute-path) inputs are bound verbatim into the "
                        f"remote container -- metasmith does NOT transfer them. Make these "
                        f"inputs resident on the remote agent host (or reference paths "
                        f"that exist there) before staging. [#240]"
                    )
                Log.Info(f"external binds {_srcs}")
            binds = mock.MakeBindsParam()
            sh_remote.Exec(f"""\
                export BINDS="{binds}"
                ./msm api stage_workflow -a task_key={task._key} verify={verify_external_paths} host=$(hostname)
            """, timeout=None, idle_timeout=idle_timeout, what="compiling the workflow on the agent")
            launcher_path = remote_work_path / AgentPaths.LAUNCHER_FILE
            res = sh_remote.Exec(
                f'[ -e {launcher_path} ] && echo "launcher-staged"', history=True, quiet=True,
                idle_timeout=PROBE_TIMEOUT, what="checking the compiled launcher",
            )
            assert "launcher-staged" in res.out, f"stage_workflow returned but launcher missing at [{launcher_path}]"

    def GetNxfConfigPresets(self, folder: Path = MODULE_PATH/"nextflow_config"):
        return GetNxfConfigPresets(folder)

    def _resolve_params(self, params: dict|Path|str|None):
        """This agent's declared params, with a run's own layered over them.

        Same reasoning and the same one place as the preset: the caller who most
        often names nothing is a person clicking launch, who has no way to know
        their cluster needs an account. A params *file* has nothing to merge
        into, so it wins whole -- said out loud rather than silently dropping
        what the agent declared.
        """
        if not self.default_params: return params
        if isinstance(params, (Path, str)):
            Log.Warn(
                f"params given as a file [{params}], so this agent's "
                f"{len(self.default_params)} default param(s) are not applied"
            )
            return params
        return dict(self.default_params) | dict(params or {})

    def RunWorkflow(
            self, 
            task: WorkflowTask|str, 
            config_file: Path|None=None, 
            params: dict|Path|str|None=None,
            resource_overrides: ResourceOverrides|None=None,
            gpus: Gpu|None=None,
            stub_delay: float=0,
        ) -> None:
        is_dry_run = stub_delay>0
        if is_dry_run:
            Log.Info(f"starting dry run")
        # The one place a preset is resolved, so the CLI, the notebook and the
        # web page all get the agent's declared default without any of them
        # knowing about it. A named preset that no longer exists is worth saying
        # out loud: a KeyError here reads as a metasmith bug rather than as a
        # line in someone's agent.yml.
        if config_file is None:
            presets = self.GetNxfConfigPresets()
            wanted = self.default_preset or "local"
            assert wanted in presets, (
                f"this agent's default nextflow preset [{wanted}] is not one of "
                f"{sorted(presets)}"
            )
            config_file = presets[wanted]
        params = self._resolve_params(params)
        task_key = task.GetKey() if isinstance(task, WorkflowTask) else task
        agent_shell = AgentShell(self)
        with agent_shell as sh_remote:
            task_path = AgentPaths.to_task(task_key, root=self.home.GetPath())
            workspace = task_path.parent.parent
            FLAG = "workspace exists"
            res = sh_remote.Exec(
                f"[ -e {workspace} ] && echo '{FLAG}'", history=True, quiet=True,
                idle_timeout=PROBE_TIMEOUT, what="checking the staged workspace",
            )
            assert FLAG in res.out, f"task not staged, expected [{workspace}] to exist"

            # GPU preflight, deliberately BEFORE anything is transferred and
            # long before the detached `nohup nextflow ... &` launch -- the run
            # is fire-and-forget, so a failure raised any later is invisible to
            # this caller.
            def _detect_gpu_on_target() -> str:
                probe = sh_remote.Exec(
                    "command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L 2>/dev/null | head -4",
                    history=True, quiet=True,
                )
                return "; ".join(x.strip() for x in probe.out if x.strip())
            gpu_manifest = _read_gpu_manifest(sh_remote, workspace)
            gpu_planned = _plan_gpu_requests(gpu_manifest, gpus, _detect_gpu_on_target)
            if gpu_planned:
                Log.Info(f"GPU requests planned for [{len(gpu_planned)}] of [{len(gpu_manifest)}] declaring steps")

            # Tool-environment preflight, same placement and same reasoning: a
            # step whose tool has no form this agent can run must be caught here
            # rather than mid-run, after everything upstream has already been
            # computed.
            env_manifest = _read_env_manifest(sh_remote, workspace)
            _check_env_portability(env_manifest, Environment(
                image=self.container, runtime=self.runtime, native=self.native,
            ))

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
                            stacks = [x for x in k.split("_") if x != ""] if "_" in k else [k]
                            if len(stacks)>1:
                                # setdefault, not assignment: two keys sharing a
                                # prefix (process_tries + process_clusterOptionsExtra)
                                # must merge into one nested dict rather than the
                                # later one wiping the earlier.
                                _d_curr = parsed
                                for _k in stacks[:-1]:
                                    _nxt = _d_curr.get(_k)
                                    if not isinstance(_nxt, dict): _nxt = {}
                                    _d_curr[_k] = _nxt
                                    _d_curr = _nxt
                                _d_curr[stacks[-1]] = v
                            else:
                                # stacks[0] rather than k so a leading/trailing
                                # underscore ("_foo") lands as "foo" instead of
                                # being silently dropped as it used to be.
                                parsed[stacks[0]] = v
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
                # GPU blocks first, so an explicit resource_overrides entry for
                # the same step is still last-defined and wins per-directive.
                if gpu_planned:
                    # Only a grid executor has a scheduler to ask; the local
                    # executor inherits whatever devices the host has, so the
                    # declaration there exists purely to pass the preflight and
                    # switch on the runtime's GPU flags.
                    is_scheduler = "slurmAccount" in local_config.read_text()
                    gpu_lines = _render_gpu_config(gpu_planned, gpus, is_scheduler)
                    if gpu_lines:
                        with open(local_config, "a") as f:
                            f.write("\n".join(gpu_lines))
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
            res = sh_remote.Exec(
                f"[ -e {launcher} ] && echo 'launcher-present'", history=True, quiet=True,
                idle_timeout=PROBE_TIMEOUT, what="checking the launcher",
            )
            assert "launcher-present" in res.out, f"launcher missing at [{launcher}]; re-stage the task"
            # the launcher detaches; the bound covers reaching that point, not the run
            sh_remote.Exec(
                f"{launcher} {stub_delay:0.3f}",
                idle_timeout=IDLE_TIMEOUT, what="launching the run",
            )

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
                res = sh_remote.Exec(
                    f"[ -e {result_path} ] && echo '{FLAG}'", history=True, quiet=True,
                    idle_timeout=PROBE_TIMEOUT, what="checking for results",
                )
                assert FLAG in res.out, f"results not found at [{self.home.ReplacePathWith(result_path).address}]"

        if self.globus_uuid is not None and allow_globus:
            src = GlobusSource(endpoint=self.globus_uuid, path=result_path).AsSource()
        else:
            src = self.home.ReplacePathWith(result_path)
        return src
