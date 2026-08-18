"""Workflow planning + task introspection."""
from __future__ import annotations

from ..models.dag_draw import DEFAULT_LABEL_CHARS, default_label
from ..models.dag_renderer import DagRenderer, LabelMode, NodeKind
from ..agents import Spec
from . import workspace as _ws


def plan_spec(spec: Spec, workspace: str | None = None, return_task: bool = False) -> dict:
    """Solve a spec, and report it the way an ops caller needs.

    Two things a notebook does not want and every veneer does: a failure
    reported as a value with the planner's hints attached rather than as a
    half-built task, and the bundle persisted under <workspace>/<task_key>/ so
    that directory becomes a task reference the CLI can stage.

    What the plan *means* -- what a sample type does, what a shared input is --
    is documented once, on `Spec.Solve`.

    `return_task=True` hands back `(result, task)` instead of just `result` --
    for a caller that needs the live, already-imported `WorkflowTask` for
    something more (the GUI draws the diagram from it) and would otherwise
    have to reload the bundle from disk, re-importing every transform in it a
    second time.
    """
    task = spec.Solve()
    plan = task.plan

    if not plan.steps or plan.dropped_targets:
        result = {
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
        return (result, task) if return_task else result

    task_key = _ws.save_task(workspace, task)
    result = {
        "success": True,
        "task_key": task_key,
        "steps": [step.Pack() for step in plan.steps],
        "targets": [t.Pack() for t in plan.targets],
        "step_count": len(plan.steps),
    }
    return (result, task) if return_task else result


def plan_workflow(
    data_library: str,
    sample_type: str | None,
    target_types: list[str | dict],
    transform_libraries: list[str],
    resource_libraries: list[str] | None = None,
    workspace: str | None = None,
    shared_input_paths: list[str] | None = None,
) -> dict:
    """`plan_spec` for a caller holding loose arguments rather than a spec.

    The CLI's door, and the shape every existing caller was written against.
    """
    return plan_spec(
        Spec(
            input_library=data_library,
            target_types=list(target_types),
            transform_libraries=list(transform_libraries),
            resource_libraries=list(resource_libraries or []),
            sample_type=sample_type,
            shared_input_paths=list(shared_input_paths or []),
        ),
        workspace=workspace,
    )


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
    label_mode: str = "column",
    show_step_order: bool = False,
    colour: str = "module",
    theme: str = "light",
) -> dict:
    task = _ws.load_task(workspace, task_key)
    # name the file with its real extension: `plan.dag` alone reads back as a
    # `.dag` suffix, which RenderDAG would take for the requested format
    out = _ws.task_path(workspace, task_key) / f"plan.dag.{format}"
    bl = set(blacklist_namespaces) if blacklist_namespaces else {"lib", "containers", "env"}
    rendered = task.plan.RenderDAG(
        out,
        blacklist_namespaces=bl,
        label_mode=LabelMode(label_mode),
        show_step_order=show_step_order,
        colour=colour,
        theme=theme,
    )
    return {"task_key": task_key, "format": format, "path": str(rendered)}


# bumped whenever `serialize_geometry`'s payload changes shape. A stored plan
# graph is a drawing frozen at write time, and the only way a reader can tell a
# stale one from a current one is to be told: testing for a key that happens to
# be new works exactly once, and then the next change has to find another key.
GEOMETRY_VERSION = 2

_WIRE_KINDS = {
    "transform": NodeKind.TRANSFORM,
    "target": NodeKind.TARGET,
    "data": NodeKind.DATA,
}


def dag_geometry(
    nodes: list[dict],
    edges: list[dict],
    label_mode: str = "column",
    font_size: float = 13.0,
    max_label_chars: int = 22,
    colour: str = "none",
    order: list[str] | None = None,
    row_y: dict[str, float] | None = None,
    min_lanes: int = 0,
) -> dict:
    """Place an arbitrary graph, in pixels, for a caller that draws it itself.

    Same engine as `render_dag` -- the rails layout and the routed, jogged,
    corner-rounded edge paths -- but stopping one step short of ink. A caller
    that wants its nodes to be clickable (the GUI's info panel) cannot use the
    SVG, and laying the graph out a second way in the browser is how the two
    drawings came to disagree about what the same plan looks like.

    An edge crosses the wire as a path and nothing else. A caller whose rows
    are already on a page of its own -- the recipe's, one per form row, at
    whatever heights the browser gave them -- says so with `order` and `row_y`
    and gets paths baked against those, rather than re-baking a published grid
    itself in a second implementation nothing holds honest.

    `nodes` are `{"id", "kind", "label"?}`; `kind` is `transform`, `target` or
    `data`, which decides the marker drawn and the trim taken off each edge end.
    `edges` are `{"from", "to"}`. Ids are the caller's and are echoed untouched.

    `order` is a node id per row, top to bottom; it is honoured only when it is
    a topological permutation of the nodes (see `dag_layout.layout`). `row_y` is
    a node id -> pixel y map, resolved against whatever rows the layout came
    back with, so the caller never has to know what row a node landed in; it is
    ignored unless every node has one. `min_lanes` is a floor on the grid width,
    so a rail's gutter does not move sideways when its first branch appears.

    `colour` names a `dag_colour` scheme; the default says nothing and each
    record's `hue` is absent. Only the plan asks for one -- skipping it is how
    the panel and the recipe's rails stay monochrome.
    """
    renderer = DagRenderer(label_mode=LabelMode(label_mode), colour=colour)
    ids = set()
    for n in nodes:
        nid = str(n["id"])
        ids.add(nid)
        kind = _WIRE_KINDS.get(str(n.get("kind") or "data"), NodeKind.DATA)
        text = n.get("label")
        renderer.add_node(kind, nid, label=default_label(str(text)) if text else None)
    for e in edges:
        src, dst = str(e["from"]), str(e["to"])
        if src in ids and dst in ids:
            renderer.add_edge(src, dst)
    return serialize_geometry(
        renderer, font_size=font_size, max_label_chars=max_label_chars,
        order=order, row_y=row_y, min_lanes=min_lanes,
    )


def serialize_geometry(
    renderer: DagRenderer,
    *,
    font_size: float = 13.0,
    max_label_chars: int = DEFAULT_LABEL_CHARS,
    order: list[str] | None = None,
    row_y: dict[str, float] | None = None,
    min_lanes: int = 0,
) -> dict:
    """A built graph's placement as plain data, for a client that draws it.

    Split out from `dag_geometry` so the plan -- whose graph is built from the
    solved steps, not from a wire payload -- reaches the same serialization
    rather than a second one that agrees only by coincidence.
    """
    # one layout, shared by the geometry and the colouring: `colouring()` runs
    # its own when passed none, and that is the whole expensive half of this
    lay = renderer.layout(order)
    rows_y: list[float] = []
    if row_y:
        ys = [row_y.get(n.name) for n in lay.nodes]
        # all or nothing: one missing row would silently draw the whole rail at
        # the nominal pitch from that row down
        if all(y is not None for y in ys):
            rows_y = [float(y) for y in ys]  # type: ignore[arg-type]
    geo = renderer.geometry(
        lay, font_size=font_size, max_label_chars=max_label_chars,
        min_lanes=min_lanes, rows_y=rows_y,
    )
    hues = renderer.colouring(lay)
    return {
        "v": GEOMETRY_VERSION,
        "width": geo.width, "height": geo.height,
        "font_size": geo.font_size, "marker_d": geo.marker_d,
        "row_pitch": geo.row_pitch, "lane_pitch": geo.lane_pitch,
        "anchor": geo.anchor,
        "nodes": [
            {
                "id": n.name, "kind": n.kind.name.lower(), "row": n.row, "lane": n.lane,
                "cx": n.cx, "cy": n.cy, "label_x": n.label_x,
                "marker_w": n.marker_w, "marker_h": n.marker_h,
                "namespace": n.namespace, "label": n.label,
                "full": n.full, "truncated": n.truncated,
                **({"hue": hues.nodes[n.name]} if n.name in hues.nodes else {}),
            }
            for n in geo.nodes
        ],
        # a back edge is reported so a caller can say the cycle exists; it has
        # no path because the rails engine does not route one
        "edges": [
            {
                "from": e.src, "to": e.dst, "back": e.back, "d": e.d,
                **({"hue": hues.edges[(e.src, e.dst)]}
                   if (e.src, e.dst) in hues.edges else {}),
            }
            for e in geo.edges
        ],
    }


def list_tasks(workspace: str | None = None) -> list[dict]:
    return _ws.list_tasks(workspace)


def delete_task(task_key: str, workspace: str | None = None) -> dict:
    return _ws.delete_task(workspace, task_key)
