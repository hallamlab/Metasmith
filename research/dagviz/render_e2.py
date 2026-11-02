#!/usr/bin/env python3
"""Render E2's plan through every DagRenderer mode, light and dark.

E2 is the parity arm of the benchmark: nf-core/mag's pipeline on the same CAMI
samples, driven by metasmith. Its plan is small enough to read whole (15 steps
short, 14 long) and wide enough to show what each mode does to a fan-out, which
is why it is the subject here rather than a shipped template.

The full arms are two solves, but that is a property of the TARGET SET rather
than of the read shapes. Ask both arms for one target they can both reach
through the generic types -- `e2::comebin_bin`, which needs `e2::assembly` and
`e2::binning_bam` and names no assembler -- and one solve carries both, with
the shared tail instantiated once per arm. `single_solve()` is that plan.

What breaks a two-case solve is a target slot only one arm can fill. Naming
`e2::assembly` gives one slot for two producers, the merge substitutes flye's
product for megahit's, and the witness catches the result as `emission`. The
full E2 target set does the same thing by naming `e2::megahit_assembly` and the
short-only FastQC/fastp reports.

The solve reads a copy of the driver's dry-run pool at `e2cache/`, so nothing
here touches the bench-run scope or the cluster. Copying the pool preserved
mtimes, which is what a leaf's instance id hashes -- but the plan KEY is still
not the published one, because the givens resolve against a different tree. The
topology is what these pictures are of, and that does not move.
"""

import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
# This worktree's own drivers, not the bench-run scope's. `_common` derives the
# library and the metasmith_libraries root from its own __file__, so whichever
# copy is imported decides the whole tree these pictures are of -- and reading a
# sibling worktree meant the artifact showed that scope's uncommitted state
# rather than this branch's. Override with MSM_DRIVERS to render another tree.
DRIVERS = Path(os.environ.get(
    "MSM_DRIVERS", HERE.parents[1] / "research/metasmith_benchmark/drivers"))
os.environ.setdefault("E2_CACHE_DIR", str(HERE / "e2cache"))
sys.path.insert(0, str(DRIVERS))

import _common as c  # noqa: E402
import e2_cami as e2  # noqa: E402
from metasmith.models.dag_renderer import (  # noqa: E402
    SYNTHETIC, DagMode, DagRenderer, Label, NodeKind,
)
from metasmith.models.solver import Endpoint  # noqa: E402
from metasmith.python_api import DataInstanceLibrary, TransformInstanceLibrary  # noqa: E402

# E2's tool environments live in its own namespace, so the engine's `env`
# namespace default misses them. Each carries `e2: env`, and a type holding only
# that property is a supertype of every one of them.
E2_ENV = Endpoint.Unpack({"properties": {"e2": "env"}})

# name -> the DagRenderer keywords that make it. Only the plain drawing is
# coloured: the `module` scheme is a hue per dominator subtree, which says
# something across a whole plan and nothing inside a three-node legend block or
# a bare step chain.
#
# `plain` is drawn per arm, because the whole point of it is one plan whole.
# `steps` and `legend` are drawn once over both arms combined: they are about
# the pipeline rather than about a run of it, and the two arms differ by one
# assembler, so a reader comparing them side by side is comparing two pictures
# that are the same everywhere except the row they want to find.
# `monochrome` rather than `colour="none"`: the scheme stays configured and the
# flag only gates it, so these two can be flipped back to hues without being
# told again which scheme they wanted.
#
# `merge` says what the union keys on, and the two combined panels want
# opposite answers. The legend is an inventory of tools, so one block per
# transform signature across both arms is exactly right. The steps view is a
# topology, and there merging on type name would fuse the two arms' binning
# tails into steps neither arm runs -- so every computed type is fenced behind
# the arm that computed it and only the givens stay shared.
PER_ARM = {"plain": dict(mode=DagMode.PLAIN, colour="module", blacklist=[E2_ENV])}
COMBINED = {
    "steps": dict(mode=DagMode.STEPS, colour="module", monochrome=True,
                  merge=False),
    "legend": dict(mode=DagMode.LEGEND, legend_columns=3, colour="module",
                   monochrome=True, merge=True, blacklist=[E2_ENV]),
}
SINGLE = dict(mode=DagMode.PLAIN, colour="module", blacklist=[E2_ENV])


def plan_for(arm: str):
    samples = e2.enumerate_arms()[arm]
    smith = c.agent_for("cami", False, Path(os.environ["E2_CACHE_DIR"]) / f"dryrun_home_{arm}")
    inputs, globals_lib = e2.declare_givens(smith, arm, samples, ensure=False)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("e2::read_metadata")),
        resources=[
            DataInstanceLibrary.Load(c.LIBRARY / "resources" / "e2"),
            DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"),
            globals_lib,
        ],
        transforms=[TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e2")],
        targets=e2.build_targets(arm),
    )
    return task.plan


def single_solve(n_per_arm: int = 1):
    """One solve over both read shapes, for a target both arms can reach.

    The samples of the two arms live in different given libraries, so they are
    handed to the solver as two sample views rather than pooled -- which is all
    `GenerateWorkflow` wants. The two arms then arrive as two unique cases, and
    the plan carries both chains.
    """
    from metasmith.python_api import TargetBuilder

    arms = e2.enumerate_arms()
    cache = Path(os.environ["E2_CACHE_DIR"])
    samples, smith, globals_lib = [], None, None
    for arm in ("short", "long"):
        agent = c.agent_for("cami", False, cache / f"dryrun_home_{arm}")
        smith = smith or agent
        inputs, globals_lib = e2.declare_givens(
            agent, arm, arms[arm][:n_per_arm], ensure=False,
        )
        samples += list(inputs.AsSamples("e2::read_metadata"))

    targets = TargetBuilder()
    targets.Add("e2::comebin_bin")
    task = smith.GenerateWorkflow(
        samples=samples,
        resources=[
            DataInstanceLibrary.Load(c.LIBRARY / "resources" / "e2"),
            DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"),
            globals_lib,
        ],
        transforms=[TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e2")],
        targets=targets,
    )
    assert task.ok, f"the single solve dropped {sorted(task.plan.dropped_targets)}"
    return task.plan


def _canonical_ids(r: DagRenderer, arm: str, merge: bool) -> dict[str, str]:
    """old node id -> an id that names the same thing in either arm's plan.

    A data node is already its own type name. A step is not: `BuildDAG` ids it
    by execution order, and the arms order differently, so the same tool lands
    on a different id in each. Naming a step by its tool plus the types it
    binds is stable across the arms and still keeps the four `checkm2` steps
    apart, since each reads a different bin type.

    `merge=False` additionally fences every computed type behind the arm that
    computed it. Both arms name their binner output `e2::comebin_bin`, so a
    union keyed on type name alone folds the two arms' binning tails into one
    -- four `checkm2` steps fed by both assemblers, which is not a step either
    arm runs. Only the givens stay shared, which is the truth: the two arms
    read the same metadata, the same references and the same tool
    environments, and compute nothing in common.
    """
    preds, succs = r._neighbours(r._nodes, r._edges)
    labels = r.labels
    out: dict[str, str] = {}

    def data_id(n: str) -> str:
        if merge or "given" in preds[n]:
            return n
        return f"{arm}\x00{n}"

    for n, kind in r._nodes.items():
        if kind is not NodeKind.TRANSFORM:
            out[n] = n if n in SYNTHETIC else data_id(n)
            continue
        if n in SYNTHETIC:
            out[n] = n
            continue
        ins = sorted(data_id(p) for p in preds[n] if r._is_data(p))
        outs = sorted(data_id(s) for s in succs[n] if r._is_data(s))
        out[n] = "\x00".join([labels[n].name, *ins, "\x00->", *outs])
    return out


def combine(sources: dict[str, DagRenderer], merge: bool = False, **kw) -> DagRenderer:
    """Union several arms' plain graphs into one renderer."""
    r = DagRenderer(**kw)
    for arm, src in sources.items():
        ids = _canonical_ids(src, arm, merge)
        labels = src.labels
        for n, kind in src._nodes.items():
            L = labels[n]
            r.add_node(kind, ids[n], Label(name=L.name, namespace=L.namespace),
                       dtype=src._dtypes.get(n))
            if kind is NodeKind.TARGET:
                # setdefault kept whatever the first arm said; a target in any
                # arm is a target of the union
                r.mark(NodeKind.TARGET, ids[n])
        for a, b in src._edges:
            r.add_edge(ids[a], ids[b])
        for n, (requires, produces) in src._declared.items():
            r.declare(ids[n], requires, produces,
                      {t: src._dtypes[t] for t in (*requires, *produces) if t in src._dtypes})
    return r


def main(arms=("short", "long")):
    out = HERE / "svg"
    out.mkdir(parents=True, exist_ok=True)

    def emit(r: DagRenderer, stem: str) -> None:
        path = r.render(out / stem, "svg")
        print(f"{path.name}  ({path.stat().st_size // 1024} KiB)")

    graphs: dict[str, DagRenderer] = {}
    for arm in arms:
        plan = plan_for(arm)
        graphs[arm] = plan.BuildDAG(mode=DagMode.PLAIN)
        for name, kw in PER_ARM.items():
            for theme in ("light", "dark"):
                # no painted background: the page picks one per colour scheme,
                # and a painted plate would show through as a pale slab in dark
                emit(
                    plan.BuildDAG(theme=theme, background=False, **kw),
                    f"e2_{arm}_{name}_{theme}",
                )

    for name, kw in COMBINED.items():
        for theme in ("light", "dark"):
            emit(
                combine(graphs, theme=theme, background=False, **kw),
                f"e2_combined_{name}_{theme}",
            )

    single = single_solve()
    print(f"single solve: {len(single.steps)} steps")
    for theme in ("light", "dark"):
        emit(
            single.BuildDAG(theme=theme, background=False, **SINGLE),
            f"e2_single_{theme}",
        )


def dump_graphs(arms=("short", "long")):
    """The graphs `cases.py` draws in the dev panel, environments cut."""
    import json

    out = HERE / "graphs"
    out.mkdir(exist_ok=True)
    for arm in arms:
        plan = plan_for(arm)
        r = plan.BuildDAG(blacklist=[E2_ENV])
        nodes, edges = r._graph()
        labels = r.labels
        path = out / f"e2_{arm}.graph.json"
        path.write_text(json.dumps({
            "arm": arm,
            "n_steps": len(plan.steps),
            "nodes": [{"name": n, "kind": k.name, "label": vars(labels[n])}
                      for n, k in nodes.items()],
            "edges": edges,
        }, indent=1), encoding="utf-8")
        print(f"{path.name}  {len(plan.steps)} steps, {len(nodes)} nodes")


if __name__ == "__main__":
    if sys.argv[1:2] == ["graphs"]:
        dump_graphs(tuple(sys.argv[2:]) or ("short", "long"))
    else:
        main(tuple(sys.argv[1:]) or ("short", "long"))
