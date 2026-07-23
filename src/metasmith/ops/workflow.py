"""Workflow planning + task introspection."""
from __future__ import annotations

from pathlib import Path

from ..models.libraries import DataInstanceLibrary, DataInstanceLibraryView
from ..models.solver import Transform, Dependency
from ..models.workflow import WorkflowPlan, WorkflowTask
from ..agents import TargetBuilder, TargetSpec
from ._common import load_data_lib, load_transform_lib
from . import workspace as _ws


def plan_workflow(
    data_library: str,
    sample_type: str,
    target_types: list[str],
    transform_libraries: list[str],
    resource_libraries: list[str] | None = None,
    workspace: str | None = None,
) -> dict:
    """Plan a workflow: chain of transforms from sample_type to target_types.

    Persists the resulting WorkflowTask under <workspace>/<task_key>/ on success.
    """
    data_lib = load_data_lib(data_library)
    samples = list(data_lib.AsSamples(sample_type))
    assert samples, f"no samples of type [{sample_type}] found in [{data_library}]"

    tr_libs = [load_transform_lib(p) for p in transform_libraries]
    res_libs = [load_data_lib(p) for p in (resource_libraries or [])]

    def _get_endpoint(dtype_name: str):
        ns, _ = dtype_name.split("::")
        for trlib in tr_libs:
            if ns not in trlib.types:
                continue
            return trlib.GetType(dtype_name)
        raise AssertionError(f"no transforms had the namespace [{ns}]")

    targets = TargetBuilder()
    for t in target_types:
        targets.Add(t)

    target_model = Transform()
    _spec2dep: dict[TargetSpec, Dependency] = {}
    target_names: list[str] = []
    for spec in targets.resolve():
        e = _get_endpoint(spec.dtype_name)
        d = target_model.AddRequirement(example=e, parents={_spec2dep[p] for p in spec.parents})
        _spec2dep[spec] = d
        target_names.append(spec.dtype_name)

    res_views = [DataInstanceLibraryView(lib) for lib in res_libs]
    plan = WorkflowPlan.Generate(
        given=[[sample] + res_views for sample in samples],
        transforms=tr_libs,
        target_names=target_names,
        target_model=target_model,
    )

    if not plan.steps or plan.dropped_targets:
        return {
            "success": False,
            "message": "solver could not find a complete plan",
            "step_count": len(plan.steps),
            "dropped_targets": list(plan.dropped_targets),
            "hints": [
                {
                    "kind": h.kind,
                    "target": h.target,
                    "message": h.message,
                    "chain": list(h.chain),
                    "candidate_transforms": list(h.candidate_transforms),
                    "near_misses": list(h.near_misses),
                }
                for h in plan.hints
            ],
        }

    task = WorkflowTask(
        ok=len(plan.dropped_targets) == 0,
        plan=plan,
        data_libraries=[data_lib] + res_libs,
        transform_libraries=tr_libs,
    )
    task_key = _ws.save_task(workspace, task)
    return {
        "success": True,
        "task_key": task_key,
        "steps": [step.Pack() for step in plan.steps],
        "targets": [t.Pack() for t in plan.targets],
        "step_count": len(plan.steps),
    }


def get_plan(task_key: str, workspace: str | None = None) -> dict:
    task = _ws.load_task(workspace, task_key)
    plan = task.plan
    return {
        "task_key": task_key,
        "ok": task.ok,
        "step_count": len(plan.steps),
        "dropped_targets": list(plan.dropped_targets),
        "steps": [s.Pack() for s in plan.steps],
        "targets": [t.Pack() for t in plan.targets],
        "given": [inst.Pack() for inst in plan.given],
    }


def get_hints(task_key: str, workspace: str | None = None) -> list[dict]:
    task = _ws.load_task(workspace, task_key)
    return [
        {
            "kind": h.kind,
            "target": h.target,
            "message": h.message,
            "chain": list(h.chain),
            "candidate_transforms": list(h.candidate_transforms),
            "near_misses": list(h.near_misses),
        }
        for h in task.plan.hints
    ]


def render_dag(
    task_key: str,
    format: str = "svg",
    blacklist_namespaces: list[str] | None = None,
    workspace: str | None = None,
) -> dict:
    task = _ws.load_task(workspace, task_key)
    out_base = _ws.task_path(workspace, task_key) / "plan.dag"
    bl = set(blacklist_namespaces) if blacklist_namespaces else {"lib", "containers", "env"}
    task.plan.RenderDAG(out_base, format, blacklist_namespaces=bl)
    rendered = out_base.with_suffix(f".{format}")
    return {"task_key": task_key, "format": format, "path": str(rendered)}


def list_tasks(workspace: str | None = None) -> list[dict]:
    return _ws.list_tasks(workspace)


def delete_task(task_key: str, workspace: str | None = None) -> dict:
    return _ws.delete_task(workspace, task_key)
