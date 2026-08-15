"""Shared plumbing for the three FabFos pipeline drivers.

``assembly.py``, ``annotation.py`` and ``ecspr.py`` each resolve ONE metasmith
workflow (build typed inputs, ask the planner, render the DAG, and optionally
run it). This module holds what all three need and nothing specific to any one
of them: locating the metasmith library, staging a reference input with a
graceful stub fallback, rendering, and driving an actual run.

STUBBED REFERENCES. Several staged inputs each driver needs (the KOfam
profiles, the ECSPr atom-mapping basis, the ``algorithm::fabfos_recovery.py``
script) either have no producer transform in the library or have no confirmed
real file on this machine yet -- see each driver's module docstring for which.
``stage_ref`` below stages the real file when one is given or found at a
default path, and falls back to an empty stub (so planning and --dag still
resolves and renders) otherwise, always reporting which happened. A run
against a stub will fail inside the container, loudly, which is correct: the
gap is real and a driver papering over it would be a false claim.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

from metasmith.python_api import (
    Agent,
    Runtime,
    DataInstanceLibrary,
    Duration,
    Resources,
    Size,
    Source,
    SourceType,
)

# src/fabfos/pipelines/common.py -> repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_PROCESSED = REPO_ROOT / "data" / "fabfos" / "processed"


def _looks_like_library(root: Path) -> bool:
    return all((root / sub).is_dir() for sub in ("data_types", "resources", "transforms"))


def resolve_library_root() -> Path:
    """Locate the metasmith library that defines the FabFos transforms.

    Resolution order mirrors the deleted ``fabfos.library.resolve_library_root``:

    1. ``$FABFOS_LIBRARY`` -- explicit override.
    2. The bundled copy a conda install ships at ``fabfos/_library``.
    3. The dev sibling module ``src/metasmith_libraries``.
    """
    override = os.environ.get("FABFOS_LIBRARY")
    if override:
        root = Path(override).expanduser().resolve()
        if not _looks_like_library(root):
            raise FileNotFoundError(
                f"FABFOS_LIBRARY=[{root}] is not a metasmith library "
                f"(missing data_types/ resources/ transforms/)"
            )
        return root

    package_dir = Path(__file__).resolve().parent.parent  # src/fabfos
    bundled = package_dir / "_library"
    if _looks_like_library(bundled):
        return bundled

    dev_sibling = package_dir.parent / "metasmith_libraries"  # src/metasmith_libraries
    if _looks_like_library(dev_sibling):
        return dev_sibling

    raise FileNotFoundError(
        "could not locate the FabFos metasmith library. Set FABFOS_LIBRARY, "
        "install the package with a bundled library, or run from a source "
        "checkout with the sibling src/metasmith_libraries module present."
    )


def stage_ref(inputs: DataInstanceLibrary, staging: Path, dtype: str, *,
              given: "str | Path | None" = None,
              default: "str | Path | None" = None,
              parents=None, verify: bool = True) -> tuple["Path | str", bool]:
    """Stage one reference-shaped input; ``given`` beats ``default`` beats a stub.

    Returns ``(path_used, is_real)``. ``is_real`` is False exactly when nothing
    on disk was found and an empty placeholder was staged instead -- callers
    collect these to print one warning list rather than failing planning.

    ``verify=False`` stages the path VERBATIM, as a string, with no local
    existence check and no stub fallback. That is what a reference living on a
    remote agent's filesystem needs: probing it here would find nothing and
    quietly substitute an empty file, and the local check is not merely useless
    on that path but actively wrong. It is also why the no-path case is a hard
    error rather than a stub -- an empty database makes most lanes produce an
    empty output and *succeed*, which ``validate_gpr`` catches one whole run
    too late. The caller that turns verification off owns proving the reference
    exists (see ``examples/_fir.check_refs``).
    """
    if not verify:
        target = given if given is not None else default
        if target is None:
            raise ValueError(
                f"stage_ref({dtype}, verify=False) with no path: there is nothing "
                f"to stage and a stub is not an option -- an empty reference makes "
                f"the lane succeed with an empty output.")
        # str, not Path: a Path is local-normalised on the way in, and an
        # absolute host path is not this machine's to normalise.
        inputs.AddItem(str(target), dtype, parents=parents or set())
        return str(target), True

    candidate = Path(given).expanduser().resolve() if given else (
        Path(default).expanduser().resolve() if default else None
    )
    if candidate is not None and candidate.exists():
        inputs.AddItem(candidate, dtype, parents=parents or set())
        return candidate, True

    stub = staging / "stubs" / dtype.replace("::", "__")
    stub.parent.mkdir(parents=True, exist_ok=True)
    stub.touch()
    inputs.AddItem(stub, dtype, parents=parents or set())
    return stub, False


def report_stubs(stage_name: str, stubs: dict[str, Path]) -> None:
    if not stubs:
        return
    print(f"\n--- {stage_name}: {len(stubs)} reference(s) staged as EMPTY STUBS ---")
    print("    (planning/DAG rendering is unaffected; a real --run will fail on these)")
    for dtype, path in sorted(stubs.items()):
        print(f"  STUB  {dtype:<40} {path}")


def render_dag(task, out: Path, *, blacklist_namespaces=frozenset({"lib", "env"})) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    task.plan.RenderDAG(out, blacklist_namespaces=set(blacklist_namespaces))
    return out.with_suffix(".svg")


def step_transform_names(task) -> set[str]:
    return {Path(step.transform._path).stem for step in task.plan.steps}


def print_plan(task) -> None:
    print(f"resolved workflow: {len(task.plan.steps)} steps")
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  [{step.order}] {Path(step.transform._path).stem} -> {prods}")


def add_execution_args(p) -> None:
    """The execution flags every driver shares, in one place.

    ``--runtime``/``--threads``/``--run`` are declared by each driver already;
    this adds the two that are about *where* the work lands rather than what it
    is -- and they must read identically across the three, because a run split
    across drivers on one cluster is the normal case.
    """
    g = p.add_argument_group("execution")
    g.add_argument("--config", metavar="NAME|PATH", default=None,
                   help="nextflow config: a built-in preset name (slurm, local) "
                        "or a path to a .nf file. Default: the runner's local one")
    g.add_argument("--require-method", metavar="ID", default=None,
                   help="refuse to run unless the live method matches ID "
                        "(full '0.4.0+abc1234' or bare '0.4.0')")
    # `Agent.container` holds an image URI under a container runtime and a CONDA
    # ENV NAME under mamba, and its default is derived from the engine hash --
    # so under `--runtime mamba` it defaults to a docker:// URI and mamba refuses
    # it with "unexpected file-system separator in environment name". The env
    # that has to be named here is the one holding metasmith itself, which is a
    # property of the machine, not of the pipeline. Hence a flag, defaulted from
    # the environment rather than hardcoded.
    g.add_argument("--agent-env", metavar="NAME",
                   default=os.environ.get("FABFOS_AGENT_ENV"),
                   help="conda env the AGENT runs in under --runtime mamba (the "
                        "one with metasmith installed); ignored otherwise. "
                        "Default: $FABFOS_AGENT_ENV")


def resolve_nxf_config(config_arg: "str | None", runtime: Runtime = Runtime.APPTAINER) -> "Path | None":
    """Resolve ``--config`` to a nextflow config path.

    ``None`` leaves the runner's local default alone; an existing path is used
    as given; anything else must name a built-in preset. A name that is neither
    raises rather than falling back, because silently running a cluster job with
    the local config is a very expensive way to find out.
    """
    if config_arg is None:
        return None
    path = Path(config_arg)
    if path.exists():
        return path.resolve()
    # The presets live with the engine, and reading them needs an Agent -- but
    # --config is resolved before the run's agent exists. This one is thrown
    # away; GetNxfConfigPresets does not touch its home.
    temp = Agent(home=Source.FromLocal(Path.cwd() / ".resolve_config_tmp"), runtime=runtime)
    presets = temp.GetNxfConfigPresets()
    if config_arg in presets:
        return presets[config_arg]
    raise FileNotFoundError(
        f"--config '{config_arg}' is neither a file path nor a known preset "
        f"(available: {', '.join(sorted(presets))})"
    )


def require_method(required: "str | None") -> None:
    """Enforce ``--require-method`` before anything is staged."""
    if not required:
        return
    from ..method import check_required

    check_required(required)


def make_agent(staging: Path, runtime: Runtime, *,
               home: "Source | None" = None,
               container: "str | None" = None,
               setup_commands: "list[str] | None" = None,
               default_preset: "str | None" = None,
               gpu_args: "list[str] | None" = None) -> Agent:
    """The local agent by default; every field a remote site needs is a keyword.

    ``Agent`` already carries all of these -- this only stopped hardcoding the
    local answers to them. The site's actual values (hostnames, accounts, module
    incantations) stay out of the package: they belong to whoever is running,
    not to the pipeline, and the shipped driver test must keep planning on a
    machine with no cluster and no ssh.

    ``container`` is an image URI under a container runtime and a CONDA ENV NAME
    under mamba -- one field, two meanings, because it answers one question:
    where does the agent itself run. See ``--agent-env``.
    """
    kwargs = {}
    if container is not None:
        kwargs["container"] = container
    if setup_commands is not None:
        kwargs["setup_commands"] = list(setup_commands)
    if default_preset is not None:
        kwargs["default_preset"] = default_preset
    if gpu_args is not None:
        kwargs["gpu_args"] = list(gpu_args)
    return Agent(home=home or Source.FromLocal(staging / "agent_home"),
                 runtime=runtime, **kwargs)


def _wait_for_run(staging: Path, task_key: str, timeout_s: int = 7200) -> Path:
    internals = staging / "agent_home" / "runs" / task_key / "_metasmith"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if internals.exists():
            log_dirs = sorted(p for p in internals.glob("logs.*") if "latest" not in p.name)
            if log_dirs:
                last_log = log_dirs[-1] / "main.log"
                if last_log.exists():
                    text = last_log.read_text(errors="ignore")
                    if "run completed at" in text:
                        return last_log
                    if "ERROR" in text and "nextflow" in text.lower():
                        tail = "".join(text.splitlines(keepends=True)[-40:])
                        raise RuntimeError(f"workflow {task_key} failed:\n{tail}")
        time.sleep(5)
    raise TimeoutError(f"workflow {task_key} did not finish within {timeout_s}s")


def run_workflow(agent: Agent, task, staging: Path, *, threads: int = 8,
                  timeout_s: int = 7200, on_exist: str = "clear",
                  config_file: "Path | None" = None,
                  params: "dict | None" = None,
                  resource_overrides: "dict | None" = None,
                  gpus=None, poll_s: float = 30.0) -> Path:
    """Stage, execute, wait, and return the run's results directory.

    The waiting half is where local and remote genuinely differ. Locally there
    is a log directory on this filesystem to poll; on an ssh-homed agent that
    directory never appears here, so polling it is a guaranteed timeout on a
    run that is going fine. ``Agent.WaitForWorkflow`` asks the agent instead and
    works either way -- ``_wait_for_run`` is kept only for the local path
    because it also surfaces a nextflow ERROR without waiting out the timeout.

    ``on_exist`` defaults to ``clear`` as before, but a resubmission wants
    ``update``: clearing destroys every cached lane, which on a cluster is hours
    of recomputation to redo work that succeeded.
    """
    assert task.ok, "cannot run a workflow that failed to plan"
    remote = agent.home.type == SourceType.SSH
    agent.Deploy()
    agent.StageWorkflow(task, on_exist=on_exist)
    agent.RunWorkflow(
        task,
        config_file=config_file,
        params=params,
        resource_overrides=resource_overrides or {
            "all": Resources(cpus=threads, memory=Size.GB(max(2, threads)), duration=Duration(hours=24)),
        },
        gpus=gpus,
    )
    if remote:
        result = agent.WaitForWorkflow(task, timeout_s=timeout_s, poll_s=poll_s)
        if result["status"] != "completed":
            tail = "\n".join(f"    {ln}" for ln in result.get("tail", []))
            raise RuntimeError(
                f"workflow {task._key} ended [{result['status']}] after "
                f"{result['elapsed_s'] / 3600:.2f} h:\n{tail}")
        agent.CheckWorkflow(task)
        return Path(agent.GetResultSource(task).GetPath())
    _wait_for_run(staging, task._key, timeout_s=timeout_s)
    agent.CheckWorkflow(task)
    return staging / "agent_home" / "runs" / task._key / "results"
