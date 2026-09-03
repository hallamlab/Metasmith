from __future__ import annotations

import os
from pathlib import Path

from ..agents import Agent
from ..bootstrap import ExecuteStep
from ..coms.terminals import LiveShell
from ..constants import AgentPaths
from ..logging import Log
from ..models.libraries import (
    DataInstance,
    ExecutionResult,
    TransformInstance,
    TransformInstanceLibrary,
)
from ..models.lineage import LinPayload
from ..models.solver import Dependency, Endpoint
from ..models.workflow import WorkflowStep
from ..models.workflow.payload import build_entry, given_index


def _load_agent(agent_home: Path | None) -> Agent:
    # The agent is the only source of the runtime, so there is nothing sensible
    # to invent when none is deployed. Same call the nextflow path makes.
    if agent_home is None:
        env_home = os.environ.get("AGENT_HOME")
        if not env_home:
            raise ValueError(
                "no agent: pass --agent-home, or set AGENT_HOME. "
                "Deploy one first -- a transform runs on an agent's runtime."
            )
        agent_home = Path(env_home)
    definition = AgentPaths.to_definition(root=agent_home.resolve())
    if not definition.exists():
        raise ValueError(
            f"[{agent_home}] is not a deployed agent: no [{definition}]."
        )
    return Agent.Load(definition)


def _resolve_transform(
    transform: Path,
) -> tuple[TransformInstanceLibrary, TransformInstance]:
    transform = Path(transform).resolve()
    if not transform.exists():
        raise ValueError(f"no transform at [{transform}]")
    lib = TransformInstanceLibrary.ResolveParentLibrary(transform)
    relative = transform.relative_to(Path(lib.location).resolve())
    inst = lib.GetTransform(relative)
    assert inst is not None, f"[{relative}] did not load from [{lib.location}]"
    return lib, inst


def _bind_inputs(
    lib: TransformInstanceLibrary,
    inst: TransformInstance,
    inputs: list[tuple[str, Path]],
) -> dict[Dependency, list[DataInstance]]:
    bindable = inst.BindableNames()

    grouped: dict[str, list[Path]] = {}
    order: list[str] = []
    for name, path in inputs:
        if name not in grouped:
            grouped[name] = []
            order.append(name)
        grouped[name].append(path)

    for name in order:
        if name in bindable:
            continue
        if name in inst._ambiguous_dep_names:
            raise ValueError(
                f"[{name}] shares its requirement with another name in [{inst.name}], "
                "so the two cannot be told apart. Give the slots distinguishable types."
            )
        if name in inst._dep_names:
            raise ValueError(
                f"[{name}] is produced by [{inst.name}], not required by it. "
                f"inputs are: {bindable}"
            )
        raise ValueError(
            f"[{inst.name}] has no input named [{name}]. inputs are: {bindable}"
        )

    dep_map: dict[Dependency, list[DataInstance]] = {}
    for name in order:
        dep = inst._dep_names[name]
        dep_map[dep] = [
            DataInstance(
                path=Path(p).resolve(),
                dtype=Endpoint(properties=set(dep.properties)),
                dtype_name=name,
                parent_lib=lib,
            )
            for p in grouped[name]
        ]

    unbound = [n for n in bindable if n not in grouped]
    if unbound:
        raise ValueError(
            f"[{inst.name}] requires inputs that were not given: {unbound}"
        )
    return dep_map


def _build_lineage(dep_map: dict[Dependency, list[DataInstance]], requires: list[Dependency]) -> dict:
    given_by_path = {
        inst.ResolvePath(): inst
        for insts in dep_map.values()
        for inst in insts
    }
    entry = build_entry([
        (
            dep_map[dep][0].dtype.key if dep_map[dep] else dep.key,
            [
                (inst.ResolvePath(), given_index(inst, given_by_path))
                for inst in dep_map[dep]
            ],
        )
        for dep in requires
    ])
    # No orchestrator routed this member and nothing promotes it to the cache,
    # so it has no member key. "-" is how the sibling harnesses say that, and it
    # sends member_token to the lineage-derived token instead of raising.
    entry[LinPayload.KEY_KEY] = "-"
    return entry


def _build_dep2output(inst: TransformInstance) -> list[dict[Dependency, Endpoint]]:
    out: list[dict[Dependency, Endpoint]] = []
    for group in inst.model.produces:
        g: dict[Dependency, Endpoint] = {}
        for dep in group:
            g[dep] = Endpoint(properties=set(dep.properties))
        out.append(g)
    return out


def RunTransform(
    transform: Path,
    inputs: list[tuple[str, Path]],
    work_dir: Path | None = None,
    agent_home: Path | None = None,
) -> ExecutionResult:
    work_dir = (work_dir or Path.cwd()).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    agent = _load_agent(agent_home)
    lib, inst = _resolve_transform(transform)

    dep_map = _bind_inputs(lib, inst, inputs)
    for group in inst.model.produces:
        for dep in group:
            dep_map.setdefault(dep, [])
    step = WorkflowStep(
        order=1,
        dependency_map=dep_map,
        transform=inst,
        transform_library=lib,
    )

    requires = list(inst.model.requires)
    lineage = _build_lineage(dep_map, requires)
    input_by_dep = dict(dep_map)
    dep2output = _build_dep2output(inst)

    original_cwd = Path.cwd()
    Log.Info(f"direct-run [{inst.name}] in [{work_dir}]")
    (work_dir / "_metasmith").mkdir(parents=True, exist_ok=True)
    os.chdir(work_dir)
    try:
        with LiveShell() as shell:
            shell.RegisterOnOut(Log.Info)
            shell.RegisterOnErr(Log.Error)
            return ExecuteStep(
                step=step,
                agent=agent,
                shell=shell,
                external_cwd=work_dir,
                task_key=work_dir.name,
                lineages=[lineage],
                input_by_dep=input_by_dep,
                dep2output=dep2output,
                params={"cpus": 1, "memory": 1, "attempt": 1},
                # direct-run is host-local: no bootstrap container, nothing bound
                # at /ws. Without this every containerized transform reports
                # success=False despite having produced its outputs, because the
                # standard `output.local.exists()` idiom checks a /ws path that
                # only exists inside the nextflow bootstrap container.
                host_local=True,
            )
    finally:
        os.chdir(original_cwd)
