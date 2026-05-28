"""Direct-run API: invoke a single transform against concrete inputs.

Skips the Nextflow workflow-generation path. Wires the transform's
declared inputs to user-supplied paths, then hands off to
`bootstrap.ExecuteStep` — the same code that the Nextflow path runs.
"""

from __future__ import annotations

import os
import socket
from hashlib import md5
from pathlib import Path

from ..agents import Agent
from ..bootstrap import ExecuteStep
from ..coms.containers import ContainerRuntime
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


def _detect_runtime() -> ContainerRuntime:
    import shutil
    if shutil.which("docker"):
        return ContainerRuntime.DOCKER
    if shutil.which("apptainer") or shutil.which("singularity"):
        return ContainerRuntime.APPTAINER
    return ContainerRuntime.DOCKER


def _load_or_make_agent(agent_home: Path | None) -> Agent:
    if agent_home is not None:
        agent_yml = agent_home / "lib" / "agent.yml"
        if agent_yml.exists():
            return Agent.Load(agent_yml)
    # Synthesize a minimal agent rooted at cwd; sufficient for path translation
    # and container invocation when no deployed agent is reachable.
    home = agent_home if agent_home is not None else Path.cwd()
    return Agent(
        home=Source.FromLocal(home),
        runtime=_detect_runtime(),
    )


def _bind_inputs(
    lib: TransformInstanceLibrary,
    inst: TransformInstance,
    inputs: list[tuple[str, Path]],
) -> dict[Dependency, list[DataInstance]]:
    """Match user-supplied (type_name, path) tuples to the transform's requires.

    A type_name binds to a dep iff its resolved Endpoint satisfies the dep
    (endpoint.IsA(dep) — endpoint.properties ⊇ dep.properties). Multiple
    entries with the same type_name fill a multi-file dep in order.
    """
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
    """Synthesise a single-batch lineage entry matching what
    Orchestrator.groovy would emit. Format mirrors TransformHarness._build_lineages.
    """
    index: dict[str, list[int]] = {}
    file_groups: list[list[str]] = []
    for dep in requires:
        insts = dep_map[dep]
        files: list[str] = []
        for inst in insts:
            p = str(inst.ResolvePath())
            files.append(p)
            h = md5(p.encode()).hexdigest()
            h_val = int(h[:15], 16)
            index.setdefault(inst.dtype.key, []).append(h_val)
        file_groups.append(files)
    index["FILES"] = file_groups
    return index


def _build_dep2output(inst: TransformInstance) -> list[dict[Dependency, Endpoint]]:
    """For each product group, synthesise an Endpoint per dep from the dep's
    own properties. The synthesised Endpoint shares the dep's key (signature
    is property-derived) and preserves any embedded `ext=...` marker that
    GetPreferredFileExtension reads.
    """
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
    """Run a single transform against concrete input files.

    Mirrors `bootstrap.StageAndRunTransform`'s execution model: connects a
    shell, hands off to ExecuteStep which builds the ExecutionContext, runs
    the protocol, and reports results. Skips the workflow-solver and
    metadata-file plumbing.

    Args:
        transform_lib: Path to the TransformInstanceLibrary directory.
        transform: Relative path of the transform .py within the library.
        inputs: list of (data type name, path) pairs, e.g.
            [("ncbi::assembly_accession", Path("acc.txt"))].
            Repeat the same type to fill a multi-file dep.
        work_dir: Output directory. Defaults to cwd.
        host: Hostname for the relay (unused in host-local mode but retained
            for future relay-bounce wiring).
        agent_home: Path to a deployed agent home (containing lib/agent.yml).
            If omitted, falls back to env AGENT_HOME, or a synthesised
            host-local agent rooted at cwd.
    """
    _ = host  # reserved for relay-bounce wiring
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
    # Seed empty lists for output deps so bootstrap's on_exit reporter can
    # look them up without KeyError. Direct-run has no pre-bound output
    # DataInstances (outputs are produced fresh by the protocol).
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
            )
    finally:
        os.chdir(original_cwd)
