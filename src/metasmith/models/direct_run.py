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
from ..models.lineage import LinPayload
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
    # `member_token` refuses an entry with no KEY, because in a workflow the
    # orchestrator stamps one before submission. A direct run has no orchestrator
    # and no cache, so "-" is the honest value: it names products from the lineage
    # index instead of from a member key. testing/transform_harness.py does the
    # same for the same reason.
    entry[LinPayload.KEY_KEY] = "-"

    # Every supplied input is an ancestor of every other. `given_index` files each item
    # under its own dtype alone, which is right in a workflow -- the orchestrator knows
    # the real ancestry -- but leaves a direct run unable to answer `SourceOf` at all,
    # since no call table would name the read pair it came from. A direct run IS one
    # coherent sample by construction, so the union is the honest reading of it, and a
    # slot given more than one item still raises AmbiguousProvenance rather than guessing.
    union = {
        k: v for k, v in entry.items()
        if k not in (LinPayload.FILES_KEY, LinPayload.PROV_KEY, LinPayload.KEY_KEY)
    }
    entry[LinPayload.PROV_KEY] = [
        [dict(union) for _ in dep_map[dep]] for dep in requires
    ]
    return entry


# The channel a slot's provenance is filed under. In a compiled workflow this is the
# Nextflow channel name and the compiler writes it into the step meta as `slk`; a direct
# run has no compiler, so the same role falls to the dtype key -- which is exactly what
# `_build_lineage` files each slot's index under, so the two sides agree by construction.
# Without it `context.SourceOf` raises, and every collecting transform that recovers a
# sample label from its inputs is unrunnable outside Nextflow.
def _build_slot_channels(
    dep_map: dict[Dependency, list[DataInstance]], requires: list[Dependency]
) -> dict[str, str]:
    return {
        dep.key: (dep_map[dep][0].dtype.key if dep_map.get(dep) else dep.key)
        for dep in requires
    }


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
    cpus: int = 1,
    memory: int = 1,
    attempt: int = 1,
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
    slot_channels = _build_slot_channels(dep_map, requires)
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
                # Nextflow would hand these down from the transform's Resources();
                # here they are the caller's to state, because the caller is the
                # only thing that knows what machine this is. The defaults are the
                # smallest legal machine rather than a useful one, so a transform
                # that sizes work off params has to be told, and `memory` is a
                # count of gigabytes to match the codegen side.
                params={"cpus": cpus, "memory": memory, "attempt": attempt},
                # direct-run is host-local: no bootstrap container, nothing bound
                # at /ws. Without this every containerized transform reports
                # success=False despite having produced its outputs, because the
                # standard `output.local.exists()` idiom checks a /ws path that
                # only exists inside the nextflow bootstrap container.
                host_local=True,
                slot_channels=slot_channels,
            )
    finally:
        os.chdir(original_cwd)
