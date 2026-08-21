from __future__ import annotations

import re
import shutil
import tempfile
from pathlib import Path
from typing import Iterable

import yaml

from ..constants import AgentPaths, MODULE_PATH
from ..env import ContainerDef, Environment, Rootfs, Runtime
from ..logging import Log
from ..models.libraries import (
    DataInstanceLibrary, DataInstanceLibraryView, Gpu, Resources,
    TransformInstanceLibrary, TransformInstanceLibraryView,
)
from ..models.remote import GlobusSource, Logistics, Source
from ..models.solver import Dependency, Transform
from ..models.workflow import WorkflowPlan, WorkflowTask
from ..coms.terminals import IDLE_TIMEOUT, PROBE_TIMEOUT
from .conda import (
    NO_RECIPE, _conda_frontend, _create_conda_envs, _find_recipes, _manifest_envs, _recipe_roots,
)
from .gpu import _plan_gpu_requests, _read_gpu_manifest, _render_gpu_config
from .images import _check_image_store, _manifest_images, _materialise_images
from .portability import _check_env_portability, _read_env_manifest, _read_env_manifest_doc
from .shell import AgentShell
from .spec import Spec
from .targets import ResourceOverrides, TargetBuilder, TargetSpec


def GetNxfConfigPresets(folder: Path = MODULE_PATH/"nextflow_config") -> dict[str, Path]:
    if not folder.exists(): raise FileNotFoundError(folder)
    presets: dict[str, Path] = {}
    for f in folder.iterdir():
        if f.is_dir(): continue
        if not f.name.endswith(".nf"): continue
        presets[f.stem] = f.absolute()
    return presets


def _render_executor_config(executor: dict|None) -> list[str]:
    # Sizing the executor through `params` does not work, and fails silently.
    #
    # The presets declare `executor { cpus = params.executor.cpus; ... }`, and
    # that block is evaluated while the config is being parsed -- at which point
    # `params` holds only what the config's own `params { }` block declared. The
    # `-params-file` merge lands afterwards, so the executor keeps the preset's
    # defaults while `params.executor` reads correctly at runtime and every
    # caller believes it was heard. An 8 GB local executor then refuses at
    # submit every step asking for more, `errorStrategy = ignore` swallows the
    # refusal, and the run reports completed having produced nothing.
    #
    # Appending a literal block to the same config file is what actually binds:
    # within one file the later assignment wins, which is the same mechanism
    # `resource_overrides` already relies on.
    if not isinstance(executor, dict) or not executor: return []
    TAB = "\t"
    lines = ["", "executor {"]
    for key, value in executor.items():
        if value is None: continue
        rendered = value if isinstance(value, (int, float)) else f"'{value}'"
        lines.append(f"{TAB}{key} = {rendered}")
    lines += ["}", ""]
    return lines


class _WorkflowOps:
    def GenerateWorkflow(
        self,
        samples: Iterable[DataInstanceLibraryView|DataInstanceLibrary],
        resources: Iterable[DataInstanceLibraryView|DataInstanceLibrary],
        transforms: list[TransformInstanceLibrary|TransformInstanceLibraryView],
        targets: TargetBuilder | list[str],
        max_iter: int=256, max_refine: int=256, seed: int=42,
    ):
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
        rootfs: Rootfs|str|None = None, prune_libraries: bool = True,
    ):
        task.RefuseIfDeferred()
        rootfs = Rootfs.Parse(rootfs) if rootfs is not None else None
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
            task.SaveAs(
                self.home.ReplacePathWith(remote_path),
                partial=task_stage_partial, prune=prune_libraries,
            )
            Log.Info(f"staging")
            mock = self._get_mock_container(task)
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
            _rootfs_arg = f" rootfs={rootfs.value}" if rootfs is not None else ""
            sh_remote.Exec(f"""\
                export BINDS="{binds}"
                ./msm api stage_workflow -a task_key={task._key} verify={verify_external_paths} host=$(hostname){_rootfs_arg}
            """, timeout=None, idle_timeout=idle_timeout, what="compiling the workflow on the agent")
            launcher_path = remote_work_path / AgentPaths.LAUNCHER_FILE
            res = sh_remote.Exec(
                f'[ -e {launcher_path} ] && echo "launcher-staged"', history=True, quiet=True,
                idle_timeout=PROBE_TIMEOUT, what="checking the compiled launcher",
            )
            assert "launcher-staged" in res.out, f"stage_workflow returned but launcher missing at [{launcher_path}]"

    def GetNxfConfigPresets(self, folder: Path = MODULE_PATH/"nextflow_config"):
        return GetNxfConfigPresets(folder)

    def _assert_staged(self, shell, task_key: str) -> Path:
        workspace = AgentPaths.to_task(task_key, root=self.home.GetPath()).parent.parent
        FLAG = "workspace exists"
        res = shell.Exec(
            f"[ -e {workspace} ] && echo '{FLAG}'", history=True, quiet=True,
            idle_timeout=PROBE_TIMEOUT, what="checking the staged workspace",
        )
        assert FLAG in res.out, f"task not staged, expected [{workspace}] to exist"
        return workspace

    def MaterialiseImages(self, task: WorkflowTask|str, force: bool=False) -> dict:
        # Fetch every tool image a staged task needs, onto this agent's host.
        #
        # The answer for a cluster whose compute nodes have no route to a
        # registry: run this from the login node, which does, and every task then
        # finds its image already in the store. It is idempotent -- a second run
        # does nothing -- because each image is skipped on the same
        # artifact-and-stamp test every task consults.
        #
        # `force` re-fetches regardless, which is what to reach for when a store
        # is suspect rather than incomplete.
        task_key = task.GetKey() if isinstance(task, WorkflowTask) else task
        with AgentShell(self) as sh_remote:
            workspace = self._assert_staged(sh_remote, task_key)
            doc = _read_env_manifest_doc(sh_remote, workspace)
            images, unknown = _manifest_images(doc)
            if unknown:
                Log.Warn(
                    f"could not tell which images [{len(unknown)}] step(s) need"
                    f" (re-stage to record them): {', '.join(unknown)}"
                )
            agent_env = Environment(
                image=self.container, runtime=self.runtime, native=self.native,
                rootfs=self.rootfs,
            )
            report = _materialise_images(
                sh_remote, images, agent_env, self.home.GetPath(),
                rootfs=doc.get("rootfs"), force=force,
            )
        return {
            "task_key": task_key,
            "runtime": self.runtime.name,
            "images": report,
            "fetched": sum(1 for r in report if not r["skipped"]),
            "already_present": sum(1 for r in report if r["skipped"]),
            "unknown": unknown,
        }

    def SetupEnvironment(
        self, task: WorkflowTask|str, force: bool=False, library: Path|str|None=None,
    ) -> dict:
        # Prepare this agent's host to run a staged task, whatever it runs tools with.
        #
        # One verb over two mechanisms, because the user's question is the same in
        # both cases and the answer is a property of the agent, not of them: a
        # container agent needs its image store filled, a mamba or native agent
        # needs its tool envs built. Both read the stage-time env manifest, and
        # both are idempotent, so this is safe to press twice.
        task_key = task.GetKey() if isinstance(task, WorkflowTask) else task
        if not (self.native or self.runtime == Runtime.MAMBA):
            return self.MaterialiseImages(task_key, force=force) | {"mode": "container"}

        with AgentShell(self) as sh_remote:
            workspace = self._assert_staged(sh_remote, task_key)
            doc = _read_env_manifest_doc(sh_remote, workspace)
            envs, no_conda, unknown = _manifest_envs(doc)
            if unknown:
                Log.Warn(
                    f"could not tell which environments [{len(unknown)}] step(s) need"
                    f" (re-stage to record them): {', '.join(unknown)}"
                )
            frontend = _conda_frontend(sh_remote)
            report = []
            if frontend is None:
                Log.Warn(
                    f"neither mamba nor conda is on this agent's PATH, so [{len(envs)}]"
                    f" tool environment(s) cannot be created here"
                )
            else:
                recipes = _find_recipes(envs, _recipe_roots(library))
                report = _create_conda_envs(
                    sh_remote, recipes, frontend, self.home.GetPath(), force=force,
                )
        return {
            "task_key": task_key,
            "runtime": "native" if self.native else self.runtime.name,
            "mode": "conda",
            "frontend": frontend,
            "needed": envs,
            "envs": report,
            "created": sum(1 for r in report if r["ok"] and not r["skipped"]),
            "already_present": sum(1 for r in report if r["skipped"]),
            "no_recipe": [r["env"] for r in report if r["reason"] == NO_RECIPE],
            "no_conda": no_conda,
            "unknown": unknown,
        }

    def _resolve_params(self, params: dict|Path|str|None):
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
            is_local_preset: bool|None=None,
        ) -> None:
        is_dry_run = stub_delay>0
        if is_dry_run:
            Log.Info(f"starting dry run")
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

            agent_env = Environment(
                image=self.container, runtime=self.runtime, native=self.native,
                rootfs=self.rootfs,
            )
            env_doc = _read_env_manifest_doc(sh_remote, workspace)
            _check_env_portability(env_doc.get("steps", {}), agent_env)

            # Image-store report, same placement, and reporting rather than
            # fetching on purpose: lazy materialisation is right on a cluster
            # whose nodes can reach a registry, and moving pulls onto every
            # launch would be a regression for everyone. On a cluster whose
            # compute nodes cannot, this is what says to run the pre-flight
            # first -- and a workspace recording no images (an older stage) is
            # simply nothing to check, exactly as above.
            _images, _unknown = _manifest_images(env_doc)
            _missing = _check_image_store(
                sh_remote, _images, agent_env, self.home.GetPath(),
                rootfs=env_doc.get("rootfs"),
            )
            if _missing:
                Log.Warn(
                    f"[{len(_missing)}] of [{len(_images)}] tool image(s) are not in this"
                    f" agent's store and will be fetched by the first task that needs each"
                    f" -- which fails on a compute node with no route to a registry."
                    f" Run `metasmith workflow materialise` on a host that can fetch:\n  "
                    + "\n  ".join(_missing)
                )
            if _unknown:
                Log.Warn(
                    f"could not tell which images [{len(_unknown)}] step(s) need"
                    f" (re-stage to record them): {', '.join(_unknown)}"
                )

            Log.Info(f"sending config and params")
            mover = Logistics()
            rel_ws = workspace.relative_to(self.home.GetPath())
            ws_dest = self.home/rel_ws
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir = Path(temp_dir)
                if params is None:
                    params = dict(nothing=None)
                parsed_params: dict = {}
                if isinstance(params, dict):
                    params_local = temp_dir/AgentPaths.NXF_PARAMS
                    def _parse(d: dict):
                        parsed = {}
                        for k, v in d.items():
                            k = str(k)
                            if isinstance(v, dict):
                                v = _parse(v)
                            stacks = [x for x in k.split("_") if x != ""] if "_" in k else [k]
                            if len(stacks)>1:
                                _d_curr = parsed
                                for _k in stacks[:-1]:
                                    _nxt = _d_curr.get(_k)
                                    if not isinstance(_nxt, dict): _nxt = {}
                                    _d_curr[_k] = _nxt
                                    _d_curr = _nxt
                                _d_curr[stacks[-1]] = v
                            else:
                                parsed[stacks[0]] = v
                        return parsed

                    parsed_params = _parse(params)
                    with open(params_local, "w") as f:
                        yaml.safe_dump(parsed_params, f)
                    params_source = Source.FromLocal(params_local)
                elif isinstance(params, Path):
                    params_source = Source.FromLocal(params)
                mover.QueueTransfer(src=params_source, dest=ws_dest/AgentPaths.NXF_PARAMS)
                local_config = temp_dir/config_file.name
                shutil.copy(config_file, local_config)
                mover.QueueTransfer(src=Source.FromLocal(local_config), dest=ws_dest/AgentPaths.NXF_CONFIG)
                # Whether this run's config descends from the local preset --
                # the caller's word on that (`is_local_preset`) wins when given,
                # since the GUI always stages preset content under a fixed
                # `preset.nf` name and the stem can no longer say so. Absent
                # that, fall back to the stem for callers that pass
                # nextflow_config/local.nf directly.
                _is_local = is_local_preset if is_local_preset is not None else config_file.stem == "local"
                if _is_local:
                    # The local preset's `executor` block is a static guess (see
                    # nextflow_config/local.nf) -- it has no way to know what the
                    # box actually has. `free -b`/`nproc` on the executing host
                    # itself, appended here, overrides that guess with the real
                    # number every run. One remote round trip, no interpreter
                    # start on the far end.
                    def _detect_host_resources() -> "tuple[int, int] | None":
                        probe = sh_remote.Exec(
                            "nproc && free -b | awk '/^Mem:/{print $2}'",
                            history=True, quiet=True,
                        )
                        lines = [x.strip() for x in probe.out if x.strip()]
                        if len(lines) < 2: return None
                        try:
                            return int(lines[0]), int(lines[1])
                        except ValueError:
                            return None
                    detected = _detect_host_resources()
                    if detected:
                        host_cpus, host_mem_bytes = detected
                        # headroom so nextflow's own pool doesn't compete with
                        # the OS and whatever else is running on the box for the
                        # last core or last slice of memory
                        cpus = max(1, host_cpus - 1)
                        mem_gb = max(1, int(host_mem_bytes / (1024**3) * 0.85))
                        with open(local_config, "a") as f:
                            f.write(f"\nexecutor {{ cpus = {cpus}; memory = '{mem_gb} GB' }}\n")
                    else:
                        Log.Warn("could not detect the local host's real cpus/memory; keeping the preset's static guess")
                # An explicit `params.executor` (e.g. a GUI-set override) is meant to
                # win over both the preset's static guess and the free -b/nproc
                # auto-detect above -- append it last so its later assignment binds,
                # rather than relying on `params.executor.cpus` inside the preset's
                # own `executor {}` block, which reads correctly at runtime but is
                # evaluated too early (before -params-file is merged) to ever apply.
                executor_lines = _render_executor_config(parsed_params.get("executor"))
                if executor_lines:
                    with open(local_config, "a") as f:
                        f.write("\n".join(executor_lines))
                if gpu_planned:
                    is_scheduler = "slurmAccount" in local_config.read_text()
                    gpu_lines = _render_gpu_config(gpu_planned, gpus, is_scheduler)
                    if gpu_lines:
                        with open(local_config, "a") as f:
                            f.write("\n".join(gpu_lines))
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
            sh_remote.Exec(
                f"{launcher} {stub_delay:0.3f}",
                idle_timeout=IDLE_TIMEOUT, what="launching the run",
            )

    def CheckWorkflow(self, task: WorkflowTask|str, run: int|None=None):
        key = task._key if isinstance(task, WorkflowTask) else str(task)
        with AgentShell(self) as sh_remote:
            index_param = ""
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
