from __future__ import annotations

import hashlib
import shutil
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import yaml

from metasmith.agents import RunWorkflow
from metasmith.constants import AgentPaths, MODULE_PATH
from metasmith.env import Runtime
from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
)
from metasmith.caching.layout import CACHE_DIR_NAME
from metasmith.models.remote import Source
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import (
    NextflowGenContext,
    WorkflowPlan,
    WorkflowTask,
)


@dataclass(frozen=True)
class RunSnapshot:
    executed_steps: tuple[str, ...]
    result_fingerprints: tuple[tuple[str, str], ...]
    cache_state: tuple[tuple[str, int], ...]
    target_manifests: tuple[tuple[str, int], ...]


def build_types_library(tmp_path: Path, type_names: Iterable[str]) -> Path:
    types = DataTypeLibrary()
    for n in type_names:
        types[n] = Endpoint(properties={n})
    tmp_path.mkdir(parents=True, exist_ok=True)
    out = tmp_path / "types.yml"
    types.Save(out)
    return out


def build_samples_library(
    tmp_path: Path,
    types_path: Path,
    *,
    count: int,
    input_type: str,
    namespace: str = "cf",
    shared_root_type: str | None = None,
) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(types_path, namespace=namespace)

    parents: list = []
    if shared_root_type is not None:
        (lib.location).mkdir(parents=True, exist_ok=True)
        (lib.location / "root.txt").write_text("shared root\n", encoding="utf-8")
        root_item = lib.AddItem(
            Path("root.txt"), f"{namespace}::{shared_root_type}"
        )
        parents = [root_item]

    for i in range(count):
        sample_id = f"sample_{i:02}"
        sample_dir = lib.location / sample_id
        sample_dir.mkdir(parents=True, exist_ok=True)
        (sample_dir / "data.txt").write_text(
            f"sample {i:02} payload\n", encoding="utf-8"
        )
        lib.AddItem(
            Path(f"{sample_id}/data.txt"),
            f"{namespace}::{input_type}",
            parents=parents or None,
        )

    lib.Save()
    return lib


def build_transform_library(
    base_dir: Path,
    types_path: Path,
    transforms: dict[str, str],
) -> TransformInstanceLibrary:
    tr_path = base_dir / "transforms.xgdb"
    tr_path.mkdir(parents=True, exist_ok=True)
    meta = tr_path / "_metadata"
    types_dir = meta / "types"
    types_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy(types_path, types_dir / "cf.yml")
    (types_dir / "transforms.yml").write_text(
        textwrap.dedent(
            """\
            schema: v1
            ontology:
              name: EDAM
              version: '1.25'
              doi: https://doi.org/10.1093/bioinformatics/btt113
              strict: false
            types:
              transform:
                properties:
                - metasmith
                - transform
            """
        ),
        encoding="utf-8",
    )

    manifest = {}
    for name, code in transforms.items():
        (tr_path / f"{name}.py").write_text(code, encoding="utf-8")
        manifest[f"{name}.py"] = {"type": "transforms::transform"}

    (meta / "index.yml").write_text(
        yaml.dump({"manifest": manifest, "schema": "v1"}),
        encoding="utf-8",
    )

    return TransformInstanceLibrary.Load(tr_path)


def identity_transform_code(
    name: str,
    input_type: str,
    output_type: str,
    *,
    cacheable: bool = True,
) -> str:
    return textwrap.dedent(
        f"""
        from pathlib import Path
        from metasmith.models.libraries import (
            TransformInstanceLibrary,
            TransformInstance,
            ExecutionContext,
            ExecutionResult,
        )
        from metasmith.models.solver import Transform

        lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
        model = Transform()
        dep = model.AddRequirement(lib.GetType("cf::{input_type}"))
        out = model.AddProduct(lib.GetType("cf::{output_type}"))

        def protocol(context: ExecutionContext):
            inp = context.Input(dep)
            payload = inp.local.read_text() if inp.local.exists() else "no input"
            out_path = Path("out_{output_type}.txt")
            out_path.write_text("step={name} type={output_type}\\n" + payload)
            return ExecutionResult(manifest=[{{out: out_path}}], success=True)

        TransformInstance(
            protocol=protocol, model=model, group_by=dep, cacheable={cacheable!r}
        )
        """
    )


def grouping_transform_code(
    name: str,
    *,
    root_type: str,
    input_type: str,
    output_type: str,
) -> str:
    return textwrap.dedent(
        f"""
        from pathlib import Path
        from metasmith.models.libraries import (
            TransformInstanceLibrary,
            TransformInstance,
            ExecutionContext,
            ExecutionResult,
        )
        from metasmith.models.solver import Transform

        lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
        model = Transform()
        root = model.AddRequirement(lib.GetType("cf::{root_type}"))
        dep = model.AddRequirement(
            lib.GetType("cf::{input_type}"), parents={{root}}
        )
        out = model.AddProduct(lib.GetType("cf::{output_type}"))

        def protocol(context: ExecutionContext):
            out_path = Path("group_{output_type}.txt")
            out_path.write_text("group step={name} type={output_type}\\n")
            return ExecutionResult(manifest=[{{out: out_path}}], success=True)

        TransformInstance(protocol=protocol, model=model, group_by=root)
        """
    )


def build_workflow_task(
    samples: DataInstanceLibrary,
    tr_lib: TransformInstanceLibrary,
    *,
    sample_type: str,
    target_specs: list[tuple[str, set[str]]],
    namespace: str = "cf",
) -> WorkflowTask:
    given = [[sv] for sv in samples.AsSamples(f"{namespace}::{sample_type}")]

    target_model = Transform()
    for _name, props in target_specs:
        target_model.AddRequirement(properties=props)

    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[tr_lib],
        target_names=[name for name, _ in target_specs],
        target_model=target_model,
    )
    assert isinstance(plan, WorkflowPlan), f"planner did not converge: {plan!r}"

    return WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=[samples],
        transform_libraries=[tr_lib],
    )


def _stage_task(task: WorkflowTask) -> tuple[str, Path, WorkflowTask]:
    key = task.GetKey()
    task_path = AgentPaths.to_task(key)
    task_path.parent.mkdir(parents=True, exist_ok=True)
    task.SaveAs(Source.FromLocal(task_path))

    staged = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])
    workspace = task_path.parent.parent
    workspace.mkdir(parents=True, exist_ok=True)

    context = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=workspace,
        external_work=workspace,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=AgentPaths.HOME_ROOT,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    )
    staged.PrepareNextflow(context)

    lib_dir = workspace / "lib"
    lib_dir.mkdir(exist_ok=True)
    shutil.copy(
        MODULE_PATH / "nextflow_config/Orchestrator.groovy",
        lib_dir / "Orchestrator.groovy",
    )
    return key, workspace, staged


def _fingerprint_file(p: Path) -> str:
    h = hashlib.blake2b(digest_size=16)
    h.update(p.read_bytes())
    return h.hexdigest()


def _collect_result_fingerprints(workspace: Path) -> tuple[tuple[str, str], ...]:
    results = workspace / "results"
    if not results.exists():
        return ()
    by_target: dict[str, list[str]] = {}
    for fp in sorted(results.rglob("*")):
        if not fp.is_file():
            continue
        rel = fp.relative_to(results)
        if rel.parts and rel.parts[0] in {"_metadata"}:
            continue
        if str(rel) == "given.csv":
            continue
        if not rel.parts:
            continue
        target = rel.parts[0]
        by_target.setdefault(target, []).append(_fingerprint_file(fp))

    rows: list[tuple[str, str]] = []
    for target, digests in sorted(by_target.items()):
        rows.append((target, ",".join(sorted(digests))))
    return tuple(rows)


def _collect_cache_state(agent_home: Path) -> tuple[tuple[str, int], ...]:
    cache_root = agent_home / CACHE_DIR_NAME
    if not cache_root.exists():
        return ()
    rows: list[tuple[str, int]] = []
    for fp in sorted(cache_root.rglob("*")):
        if fp.is_file():
            rows.append((str(fp.relative_to(cache_root)), fp.stat().st_size))
    return tuple(rows)


def _collect_executed_steps(events: list[dict]) -> tuple[str, ...]:
    out: list[str] = []
    for e in events:
        if e.get("type") == "bootstrap_call":
            out.append(str(e.get("step_name", "")))
    return tuple(out)


def _collect_target_manifests(workspace: Path) -> tuple[tuple[str, int], ...]:
    trace_path = workspace / "_metasmith" / "trace.jsonl"
    if not trace_path.exists():
        return ()
    from metasmith.telemetry import TraceIndex

    trace = TraceIndex.read(trace_path)
    counts: dict[str, int] = {}
    for ev in trace.events:
        if ev.status not in ("promoted", "hit", "miss"):
            continue
        for pf in ev.produces:
            if not pf.dtype_key:
                continue
            counts[pf.dtype_key] = counts.get(pf.dtype_key, 0) + 1
    return tuple(sorted(counts.items()))


def capture_run(virtual_runtime, task: WorkflowTask) -> RunSnapshot:
    key, workspace, _staged = _stage_task(task)
    RunWorkflow(
        key=key,
        log_dir=Path("_metasmith/logs.virtual"),
        host=virtual_runtime.host,
        stub_delay=0.0,
    )
    events = virtual_runtime.parse_trace()

    return RunSnapshot(
        executed_steps=_collect_executed_steps(events),
        result_fingerprints=_collect_result_fingerprints(workspace),
        cache_state=_collect_cache_state(virtual_runtime.home),
        target_manifests=_collect_target_manifests(workspace),
    )


def clear_trace(virtual_runtime) -> None:
    if virtual_runtime.trace_file.exists():
        virtual_runtime.trace_file.unlink()
