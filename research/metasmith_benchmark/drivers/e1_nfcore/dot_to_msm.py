"""Convert a nextflow `-with-dag *.dot` graph into metasmith's bipartite DagRenderer: processes become transforms, named channels become data."""
import re, sys, collections
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3] / "src"))
from metasmith.models.dag_renderer import DagRenderer, NodeKind, Label

BOOKKEEPING = re.compile(r"versions|multiqc", re.I)


def load(path):
    attrs, edges = {}, []
    for ln in open(path):
        if m := re.match(r'\s*(v\d+) -> (v\d+)(?: \[label="([^"]*)"\])?;', ln):
            edges.append((m[1], m[2], m[3]))
        elif m := re.match(r'\s*(v\d+) \[(.*)\];', ln):
            attrs[m[1]] = m[2]
    return attrs, edges


def main(dot, out):
    attrs, edges = load(dot)
    is_proc = lambda v: "shape=" not in attrs.get(v, "")
    proc_name = lambda v: re.search(r'label="([^"]*)"', attrs[v])[1].split(":")[-1]
    xlabel = lambda v: (re.search(r'xlabel="([^"]*)"', attrs.get(v, "")) or [None, ""])[1]
    succ = collections.defaultdict(list)
    pred = collections.defaultdict(list)
    for s, d, l in edges:
        succ[s].append((d, l))
        pred[d].append(s)

    def follow(node, label):
        """Walk operators from one channel edge: first channel name seen, and processes reached."""
        name, consumers, seen, frontier = label, set(), set(), [(node, label)]
        while frontier:
            nxt = []
            for v, l in frontier:
                name = name or l
                if v in seen: continue
                seen.add(v)
                if is_proc(v):
                    consumers.add(v)
                    continue
                nxt.extend(succ[v])
            frontier = nxt
        return name, consumers

    r = DagRenderer(colour="module")
    procs = [v for v in attrs if is_proc(v) and proc_name(v) != "MULTIQC"]
    for p in procs:
        r.add_node(NodeKind.TRANSFORM, proc_name(p), Label(name=proc_name(p).lower()))

    def add_data(src_label, name, consumers):
        consumers = [c for c in consumers if proc_name(c) != "MULTIQC"]
        if not consumers or BOOKKEEPING.search(name): return
        data = f"{src_label}::{name}"
        for c in consumers:
            r.add_edge(data, proc_name(c))
        return data

    produced = set()
    for p in procs:
        for d, l in succ[p]:
            name, consumers = follow(d, l)
            if data := add_data(proc_name(p).lower(), name or "out", consumers):
                r.add_edge(proc_name(p), data)
                produced.add(data)

    r.add_node(NodeKind.TRANSFORM, "given")
    for v in attrs:
        if pred[v] or is_proc(v) or xlabel(v) == "channel.empty": continue
        for d, l in succ[v]:
            name, consumers = follow(d, l)
            if not (xlabel(v).startswith("channel.") or (name or "").startswith("ch_")): continue
            if not any(proc_name(c) != "MULTIQC" for c in consumers): continue
            if data := add_data("given", name or xlabel(v) or "input", consumers):
                r.add_edge("given", data)

    sinks = {n for n in produced if r.out_degree(n) == 0}
    for p in procs:
        if r.out_degree(proc_name(p)) == 0:
            leaf = f"{proc_name(p).lower()}::out"
            r.add_edge(proc_name(p), leaf)
            sinks.add(leaf)
    for n in sinks: r.mark(NodeKind.TARGET, n)

    print(r.render(out), len(procs), "processes", len(r._edges), "edges")
    return r


if __name__ == "__main__":
    r = main(sys.argv[1], sys.argv[2])
    kinds = r._nodes
    bad = [(a, b) for a, b in r._edges if (kinds[a] is NodeKind.TRANSFORM) == (kinds[b] is NodeKind.TRANSFORM)]
    adj = collections.defaultdict(set)
    for a, b in r._edges: adj[a].add(b); adj[b].add(a)
    seen, stack = set(), ["given"]
    while stack:
        x = stack.pop()
        if x not in seen: seen.add(x); stack.extend(adj[x])
    print("non-alternating edges:", bad)
    print("disconnected:", sorted(set(kinds) - seen))
    print("data:", sorted(n for n, k in kinds.items() if k is not NodeKind.TRANSFORM))
