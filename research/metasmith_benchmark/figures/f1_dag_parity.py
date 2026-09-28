"""F1: nf-core/mag's workflow DAG beside metasmith's solved plan, per arm, and the tool-level edge check.

E1's graph is nextflow's own `-with-dag` output (results/e1/dag/), projected to the processes that ran on
that arm. E2's graph is the plan `drivers/e2_cami.py` solves, which is the plan that ran: CheckM2 on all four
bin sets. Both are drawn in full and steps-only by the same DagRenderer, every step coloured by its function
from one palette, so a function reads the same in all eight graphs and nf-core's bookkeeping stands out
in grey. E2's two scoring steps against the CAMI gold standard, and the inputs only they read, are cut out:
nf-core has no counterpart to them.

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

# the page's categorical slots 1-4 in pipeline order, stepped per theme; bookkeeping is neutral
STAGES = {
    "read processing and QC": ("#2a78d6", "#3987e5"),
    "assembly and read mapping": ("#eb6834", "#d95926"),
    "binning and refinement": ("#1baf7a", "#199e70"),
    "bin quality": ("#eda100", "#c98500"),
    "nf-core bookkeeping and reports": ("#8a8a8a", "#7a7f88"),
}
TRANSFORM_STAGE = {
    "fastqc_raw": "read processing and QC", "fastp": "read processing and QC",
    "fastqc_trimmed": "read processing and QC", "porechop_abi": "read processing and QC",
    "chopper": "read processing and QC",
    "megahit": "assembly and read mapping", "flye": "assembly and read mapping",
    "bowtie2_binning_bam": "assembly and read mapping", "minimap2_binning_bam": "assembly and read mapping",
    "metabat2": "binning and refinement", "semibin2": "binning and refinement", "comebin": "binning and refinement",
    "das_tool": "binning and refinement", "checkm2": "bin quality",
}
SCORING = {"gold_standard", "amber"}

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


def e1_graph(arm, mode=DagMode.PLAIN, **kw):
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

    r = DagRenderer(mode=mode, **kw)
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


def e2_graph(arm, mode=DagMode.PLAIN, **kw):
    r = render_e2.plan_for(arm).BuildDAG(mode=mode, colour="none", blacklist=[render_e2.E2_ENV], **kw)
    drop_scoring(r)
    tools = {n: r.labels[n].name for n, k in r._nodes.items() if k is NodeKind.TRANSFORM and n != "given"}
    return r, tools


def drop_scoring(r):
    """Cut the scoring steps, then every data node left feeding nothing that no kept step produced."""
    for n, k in list(r._nodes.items()):
        if k is NodeKind.TRANSFORM and r.labels[n].name in SCORING:
            r.remove_node(n)
    while True:
        made = {b for a, b in r._edges if r._nodes[a] is NodeKind.TRANSFORM and a != "given"}
        dead = [n for n, k in r._nodes.items()
                if k is not NodeKind.TRANSFORM and r.out_degree(n) == 0 and n not in made]
        if not dead:
            return
        for n in dead:
            r.remove_node(n)


def paint(r, stage, theme):
    """Colour every step by its stage and every product by its producer's."""
    nodes = {n: STAGES[s][theme == "dark"] for n, s in stage.items()}
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
            for mode, tag in ((DagMode.PLAIN, ""), (DagMode.STEPS, "_steps")):
                kw = dict(theme=theme, background=False, mode=mode)
                r1, t1, s1 = e1_graph(arm, **kw)
                r2, t2 = e2_graph(arm, **kw)
                paint(r1, s1, theme).render(OUT / f"e1_{arm}{tag}_{theme}", "svg")
                paint(r2, {n: TRANSFORM_STAGE[t] for n, t in t2.items()}, theme).render(OUT / f"e2_{arm}{tag}_{theme}", "svg")
            r1, t1, _ = e1_graph(arm)
            r2, t2 = e2_graph(arm)

        e1 = tool_edges(r1, t1)
        e2 = tool_edges(r2, t2)
        c1, c2 = closure(e1), closure(e2)
        for a, b in sorted(e1 | e2):
            where = "both" if (a, b) in e1 and (a, b) in e2 else ("E1" if (a, b) in e1 else "E2")
            implied = {"E1": (a, b) in c2, "E2": (a, b) in c1}.get(where, True)
            rows.append(dict(arm=arm, src=a, dst=b, edge_in=where, implied_by_other=implied))
        n_proc = sum(1 for n, k in r1._nodes.items() if k is NodeKind.TRANSFORM and n != "given")
        n_step = sum(1 for n, k in r2._nodes.items() if k is NodeKind.TRANSFORM and n != "given")
        tools1 = {t for t in t1.values() if t}
        tools2 = set(t2.values())
        print(f"{arm}: E1 {n_proc} processes ran, {sum(1 for t in t1.values() if t)} tool-bearing -> {len(tools1)} transforms;"
              f" E2 {n_step} steps -> {len(tools2)} tools")
        summary[arm] = dict(e1_processes=n_proc, e1_tool_processes=sum(1 for t in t1.values() if t),
                            e2_steps=n_step, tools=len(tools1), same_tools=tools1 == tools2,
                            checkm2_steps=sum(1 for t in t2.values() if t == "checkm2"),
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
