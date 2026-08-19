from __future__ import annotations

import os
import socket
from pathlib import Path

from ..agents import Agent
from ..bootstrap import ExecuteStep
from ..env import Environment
from ..coms.terminals import LiveShell
from ..constants import AgentPaths
from ..logging import Log
from ..models.libraries import (
    DataInstance,
    ExecutionResult,
    TransformInstance,
    TransformInstanceLibrary,
)
from ..models.remote import Source
from ..models.solver import Dependency, Endpoint
from ..models.workflow import WorkflowStep
from ..models.workflow.payload import build_entry, given_index


def _load_or_make_agent(agent_home: Path | None) -> Agent:
    if agent_home is not None:
        agent_yml = agent_home / "lib" / "agent.yml"
        if agent_yml.exists():
            return Agent.Load(agent_yml)
    home = agent_home if agent_home is not None else Path.cwd()
    return Agent(
        home=Source.FromLocal(home),
        runtime=Environment.Detect(),
    )


def _bind_inputs(
    lib: TransformInstanceLibrary,
    inst: TransformInstance,
    inputs: list[tuple[str, Path]],
) -> dict[Dependency, list[DataInstance]]:
    by_type: dict[str, list[Path]] = {}
    type_order: list[str] = []
    endpoints: dict[str, Endpoint] = {}
    for type_name, path in inputs:
        if type_name not in by_type:
            by_type[type_name] = []
            type_order.append(type_name)
            endpoints[type_name] = lib.GetType(type_name)
        by_type[type_name].append(path)

    dep_map: dict[Dependency, list[DataInstance]] = {}
    used: set[str] = set()
    for dep in inst.model.requires:
        candidates = [t for t in type_order if t not in used and endpoints[t].IsA(dep)]
        if not candidates:
            raise ValueError(
                f"no input provided for dependency [{dep}] with properties {sorted(dep.properties)}; "
                f"supplied types were {type_order}"
            )
        chosen = candidates[0]
        used.add(chosen)
        paths = by_type[chosen]
        endpoint = endpoints[chosen]
        dep_map[dep] = [
            DataInstance(
                path=Path(p).resolve(),
                dtype=endpoint,
                dtype_name=chosen,
                parent_lib=lib,
            )
            for p in paths
        ]

    unused = [t for t in type_order if t not in used]
    if unused:
        raise ValueError(
            f"these input types were supplied but not declared by the transform: {unused}"
        )
    return dep_map


def _build_lineage(dep_map: dict[Dependency, list[DataInstance]], requires: list[Dependency]) -> dict:
    given_by_path = {
        inst.ResolvePath(): inst
        for insts in dep_map.values()
        for inst in insts
    }
    return build_entry([
        (
            dep_map[dep][0].dtype.key if dep_map[dep] else dep.key,
            [
                (inst.ResolvePath(), given_index(inst, given_by_path))
                for inst in dep_map[dep]
            ],
        )
        for dep in requires
    ])


def _build_dep2output(inst: TransformInstance) -> list[dict[Dependency, Endpoint]]:
    out: list[dict[Dependency, Endpoint]] = []
    for group in inst.model.produces:
        g: dict[Dependency, Endpoint] = {}
        for dep in group:
            g[dep] = Endpoint(properties=set(dep.properties))
        out.append(g)
    return out


def RunTransform(
    transform_lib: Path,
    transform: str,
    inputs: list[tuple[str, Path]],
    work_dir: Path | None = None,
    host: str | None = None,
    agent_home: Path | None = None,
) -> ExecutionResult:
    _ = host
    work_dir = (work_dir or Path.cwd()).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    if agent_home is None:
        env_home = os.environ.get("AGENT_HOME")
        if env_home:
            agent_home = Path(env_home)

    lib = TransformInstanceLibrary.Load(transform_lib)
    inst = lib.GetTransform(transform)
    assert inst is not None, f"transform [{transform}] not found in [{transform_lib}]"

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

    agent = _load_or_make_agent(agent_home)

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
