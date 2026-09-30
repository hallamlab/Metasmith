"""F1: nf-core/mag's workflow DAG beside metasmith's solved plan, per arm, and the tool-level edge check.

E1's graph is nextflow's own `-with-dag` output (results/e1/dag/), projected to the processes that ran on
that arm. E2's graph is the plan `drivers/e2_cami.py` solves, which is the plan that ran: CheckM2 on all four
bin sets. Both are drawn by the same DagRenderer with one hue per pipeline stage, so a reader matches the two
pictures by colour.

The edge check contracts both graphs to the tool-bearing steps E2 has a transform for, then compares edges
and reachability. Writes figures/f1/.
"""

import contextlib
import csv
import io
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
BENCH = HERE.parent
REPO = BENCH.parents[1]
OUT = HERE / "f1"
sys.path[:0] = [str(REPO / "src"), str(BENCH / "drivers" / "e1_nfcore"), str(REPO / "research" / "dagviz")]
os.environ.setdefault("E2_CACHE_DIR", str(BENCH / "drivers" / ".cache" / "e2"))

import dot_to_msm  # noqa: E402
import render_e2  # noqa: E402
from metasmith.models.dag_colour import Colouring  # noqa: E402
from metasmith.models.dag_renderer import DagMode, DagRenderer, NodeKind  # noqa: E402

STAGES = {
    "read QC": "#636EFA",
    "assembly": "#EF553B",
    "read mapping": "#00CC96",
    "binning": "#AB63FA",
    "refinement": "#FFA15A",
    "bin quality": "#19D3F3",
    "E2 scoring": "#FF6692",
    "nf-core bookkeeping and reports": "#8A8A8A",
}
TRANSFORM_STAGE = {
    "fastqc_raw": "read QC", "fastp": "read QC", "fastqc_trimmed": "read QC",
    "porechop_abi": "read QC", "chopper": "read QC",
    "megahit": "assembly", "flye": "assembly",
    "bowtie2_binning_bam": "read mapping", "minimap2_binning_bam": "read mapping",
    "metabat2": "binning", "semibin2": "binning", "comebin": "binning",
    "das_tool": "refinement", "checkm2": "bin quality",
    "gold_standard": "E2 scoring", "amber": "E2 scoring",
}

# nextflow's DAG names a process by its module; the trace table names it by its last segment
DOT_TO_TRACE = {
    "COMEBIN_RUNCOMEBIN": "COMEBIN", "DASTOOL_DASTOOL": "DASTOOL",
    "DASTOOL_FASTATOCONTIG2BIN": "FASTATOCONTIG2BIN", "METABAT2_METABAT2": "METABAT2",
    "GUNZIP_SHORTREAD_ASSEMBLIES": "GUNZIP", "GUNZIP_LONGREAD_ASSEMBLIES": "GUNZIP",
    "METABAT2_JGISUMMARIZEBAMCONTIGDEPTHS_SHORTREAD": "JGISUMMARIZEBAMCONTIGDEPTHS",
    "METABAT2_JGISUMMARIZEBAMCONTIGDEPTHS_LONGREAD": "JGISUMMARIZEBAMCONTIGDEPTHS",
}
OTHER_ARM = {"short": "LONGREAD", "long": "SHORTREAD"}


def trace(arm):
    """dot process name -> its E2 transform ('' for none), for the processes that completed a task on this arm."""
    rows = {r["process"]: r for r in csv.DictReader(open(BENCH / "results/e1/e1_runtime_by_process.tsv"), delimiter="\t")
            if r["arm"] == arm and r["n_tasks"] not in ("", "0")}
    return rows


def e1_graph(arm, **kw):
    with contextlib.redirect_stdout(io.StringIO()):
        full = dot_to_msm.main(BENCH / f"results/e1/dag/e1_{arm}.dot", OUT / f".e1_{arm}_full")
    (OUT / f".e1_{arm}_full.svg").unlink()
    ran = trace(arm)
    kinds, labels = full._nodes, full.labels
    preds = defaultdict(set)
    for a, b in full._edges:
        preds[b].add(a)

    def keep_proc(p):
        if p == "given":
            return True
        if OTHER_ARM[arm] in p:
            return False
        return DOT_TO_TRACE.get(p, p) in ran

    procs = {n for n, k in kinds.items() if k is NodeKind.TRANSFORM and keep_proc(n)}
    edges = [(a, b) for a, b in full._edges
             if (a in procs or kinds[a] is not NodeKind.TRANSFORM) and (b in procs or kinds[b] is not NodeKind.TRANSFORM)]
    edges = [(a, b) for a, b in edges if a in procs or preds[a] & procs]
    consumed = {a for a, b in edges if b in procs}
    # a given channel whose every consumer ran on the other arm has gone with them
    edges = [(a, b) for a, b in edges if not (a == "given" and b not in consumed)]

    r = DagRenderer(mode=DagMode.PLAIN, **kw)
    for a, b in edges:
        for n in (a, b):
            r.add_node(kinds[n], n, labels.get(n))
        r.add_edge(a, b)
    for n in r._nodes:
        if kinds[n] is not NodeKind.TRANSFORM and r.out_degree(n) == 0:
            r.mark(NodeKind.TARGET, n)

    to_transform = {p: ran[DOT_TO_TRACE.get(p, p)]["e2_transform"] for p in procs if p != "given"}
    stage = {p: TRANSFORM_STAGE.get(t, "nf-core bookkeeping and reports") for p, t in to_transform.items()}
    return r, to_transform, stage


def e2_graph(arm, **kw):
    plan = render_e2.plan_for(arm)
    r = plan.BuildDAG(mode=DagMode.PLAIN, blacklist=[render_e2.E2_ENV], **kw)
    tools = {n: r.labels[n].name for n, k in r._nodes.items() if k is NodeKind.TRANSFORM and n != "given"}
    return r, tools, {n: TRANSFORM_STAGE[t] for n, t in tools.items()}


def paint(r, stage):
    """Colour every step by its stage and every product by its producer's."""
    nodes = {n: STAGES[s] for n, s in stage.items()}
    for a, b in r._edges:
        if a in nodes and b not in nodes and r._nodes[b] is not NodeKind.TRANSFORM:
            nodes[b] = nodes[a]
    edges = {(a, b): nodes[a] for a, b in r._edges if a in nodes}
    r.colouring = lambda lay=None: Colouring(nodes=nodes, edges=edges)
    return r


def tool_edges(r, to_tool):
    """Step-to-step edges between tool-bearing steps, walking through data and through steps with no tool."""
    succ = defaultdict(set)
    for a, b in r._edges:
        succ[a].add(b)
    out = set()
    for src, t in to_tool.items():
        if not t:
            continue
        seen, stack = set(), list(succ[src])
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            u = to_tool.get(n)
            if u and u != t:
                out.add((t, u))
            elif not u:
                stack.extend(succ[n])
            elif u == t:
                stack.extend(succ[n])   # the second half of a 2-in-1 transform
    return out


def closure(edges):
    succ = defaultdict(set)
    for a, b in edges:
        succ[a].add(b)
    out = set()
    for a in list(succ):
        stack, seen = list(succ[a]), set()
        while stack:
            n = stack.pop()
            if n not in seen:
                seen.add(n)
                stack.extend(succ[n])
        out |= {(a, n) for n in seen}
    return out


def reduction(edges):
    c = closure(edges)
    return {(a, b) for a, b in edges if not any((a, m) in c and (m, b) in c for m in {x for _, x in c})}


def main():
    OUT.mkdir(exist_ok=True)
    rows, summary = [], {}
    for arm in ("short", "long"):
        for theme in ("light", "dark"):
            kw = dict(theme=theme, background=False)
            r1, t1, s1 = e1_graph(arm, **kw)
            r2, t2, s2 = e2_graph(arm, **kw)
            paint(r1, s1).render(OUT / f"e1_{arm}_{theme}", "svg")
            paint(r2, s2).render(OUT / f"e2_{arm}_{theme}", "svg")

        scoring = {"gold_standard", "amber"}
        e1 = tool_edges(r1, t1)
        e2 = {(a, b) for a, b in tool_edges(r2, t2) if a not in scoring and b not in scoring}
        c1, c2 = closure(e1), closure(e2)
        for a, b in sorted(e1 | e2):
            where = "both" if (a, b) in e1 and (a, b) in e2 else ("E1" if (a, b) in e1 else "E2")
            implied = {"E1": (a, b) in c2, "E2": (a, b) in c1}.get(where, True)
            rows.append(dict(arm=arm, src=a, dst=b, edge_in=where, implied_by_other=implied))
        n_proc = sum(1 for n, k in r1._nodes.items() if k is NodeKind.TRANSFORM and n != "given")
        n_step = sum(1 for n, k in r2._nodes.items() if k is NodeKind.TRANSFORM and n != "given")
        tools1 = {t for t in t1.values() if t}
        tools2 = set(t2.values()) - scoring
        print(f"{arm}: E1 {n_proc} processes ran, {sum(1 for t in t1.values() if t)} tool-bearing -> {len(tools1)} transforms;"
              f" E2 {n_step} steps -> {len(tools2)} tools (+{len(set(t2.values()) & scoring)} scoring)")
        summary[arm] = dict(e1_processes=n_proc, e1_tool_processes=sum(1 for t in t1.values() if t),
                            e2_steps=n_step, tools=len(tools1), same_tools=tools1 == tools2,
                            checkm2_steps=sum(1 for t in t2.values() if t == "checkm2"),
                            scoring_steps=sum(1 for t in t2.values() if t in scoring),
                            same_reachability=c1 == c2)
        print(f"  same tools: {tools1 == tools2}   E1-only {sorted(tools1 - tools2)}   E2-only {sorted(tools2 - tools1)}")
        print(f"  tool edges: both {len(e1 & e2)}, E1-only {len(e1 - e2)}, E2-only {len(e2 - e1)}")
        print(f"  reachability identical: {c1 == c2}   transitive reduction identical: {reduction(e1) == reduction(e2)}")
        for a, b in sorted(e1 ^ e2):
            print(f"    {'E1' if (a, b) in e1 else 'E2'}-only {a} -> {b}  implied by the other's paths: {(a, b) in (c2 if (a, b) in e1 else c1)}")

    (OUT / "summary.json").write_text(json.dumps(summary, indent=1))
    with open(OUT / "edge_check.tsv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), delimiter="\t")
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
