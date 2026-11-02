import argparse
import json
import os
import pickle
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from atom_graph import build_atom_graph, restrict_to_giant           # noqa: E402
from ieff_ground import (GroundSystem, SymmetricField, cone_solve,   # noqa: E402
                         SolveRefused)

STAR_PICKLE = Path(__file__).resolve().parent / "mnx_universe_base_C.pkl"
XREF = (Path(__file__).resolve().parents[4]
        / "data/fabfos/originals/metanetx/4.5/reac_xref.tsv")


def star_medium():
    import networkx as nx
    g0 = pickle.load(open(STAR_PICKLE, "rb"))
    g0 = g0.subgraph(max(nx.connected_components(g0), key=len)).copy()
    return {x[1] for x in g0.nodes if x[0] == "rxn"}


def kegg_annotated():
    return {p[1] for p in (l.rstrip("\n").split("\t") for l in open(XREF)
                           if not l.startswith("#"))
            if len(p) >= 2 and p[0].startswith("kegg.reaction:") and p[1] != "EMPTY"}


def gpr_medium(table):
    return set(pd.read_parquet(table).mnxr.dropna().unique())


def build(scale, gpr_table=None):
    if scale == "medium":
        g, term, _ = build_atom_graph("C", star_medium())
    elif scale == "kegg":
        g, term, _ = build_atom_graph("C", star_medium() | kegg_annotated())
    elif scale == "gpr":
        if gpr_table is None:
            raise SystemExit("--scale gpr needs --gpr-table")
        g, term, _ = build_atom_graph("C", gpr_medium(gpr_table))
    else:
        g, term, _ = build_atom_graph("C", None)
    return restrict_to_giant(g, term)


def proximity_order(S, names):
    import scipy.sparse as sp
    idx = {r: i for i, r in enumerate(names)}
    rows, cols = [], []
    for r in names:
        subs, prods = S.terminals[r]
        for a in subs | prods:
            rows.append(idx[r])
            cols.append(a)
    M = sp.csr_matrix((np.ones(len(rows)), (rows, cols)), shape=(len(names), S.n))
    A = (M @ M.T).tocsr()
    from scipy.sparse.csgraph import breadth_first_order, connected_components
    nc, lab = connected_components(A, directed=False)
    out = []
    for c in range(nc):
        mem = np.flatnonzero(lab == c)
        if len(mem) == 1:
            out.append(mem[0])
        else:
            out.extend(breadth_first_order(A, int(mem[0]), directed=False,
                                           return_predecessors=False).tolist())
    return [names[i] for i in out]


class Store:
    def __init__(self, path, src_names, rxn_names, K, mode="r+", provenance=None,
                 channels=()):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.n, self.K = len(src_names), K
        self.channels = tuple(channels)
        meta = self.path / "meta.json"
        if not meta.exists():
            json.dump(dict(src=list(src_names), rxn=list(map(str, rxn_names)), K=K,
                           **(provenance or {})), open(meta, "w"))

        def mm(name, dtype, shape):
            p = self.path / name
            return np.memmap(p, dtype, "w+" if not p.exists() else mode, shape=shape)

        self.idx = mm("idx.i32", np.int32, (self.n, K))
        self.val = mm("val.f32", np.float32, (self.n, K))
        self.rowsum = mm("rowsum.f64", np.float64, (self.n,))
        self.done = mm("done.u8", np.uint8, (self.n,))
        self.nit = mm("nit.i16", np.int16, (self.n,))
        self.ch = {c: mm(f"{c}.f32", np.float32, (self.n, K)) for c in self.channels}

    def put(self, i, row, nit, aux=None):
        K = self.K
        if K >= len(row):
            sel = np.arange(len(row))
        else:
            top = np.argpartition(row, -K)[-K:]
            sel = top[np.argsort(row[top])[::-1]]
        self.idx[i, :len(sel)] = sel.astype(np.int32)
        self.val[i, :len(sel)] = row[sel].astype(np.float32)
        for c, a in self.ch.items():
            a[i, :len(sel)] = np.asarray(aux[c], float)[sel].astype(np.float32)
        self.rowsum[i] = float(row.sum())
        self.nit[i] = nit
        self.done[i] = 1

    def flush(self):
        for a in (self.idx, self.val, self.rowsum, self.done, self.nit,
                  *self.ch.values()):
            a.flush()


def run_block(args, S, F, order, store, lo, hi, tag=""):
    t0 = time.time()
    times, phi_prev, nrefuse = [], None, 0
    reuse = S.full_reuse() if args.mode == "full" else None
    nchecked = 0
    for i in range(lo, hi):
        if store.done[i]:
            continue
        a = order[i]
        t = time.time()
        aux = None
        try:
            if args.mode == "cone":
                row, info = cone_solve(S, a, F, cover=args.cover)
                nit = info["nit"]
            elif store.channels:
                want = args.channel_check and nchecked < 3
                row, aux, phi, nit, _why = S.solve_full_channels(
                    a, reuse, phi0=phi_prev, check=want)
                phi_prev = phi if args.warm else None
                if want:
                    c = aux["check"]
                    nchecked += 1
                    print(f"{tag}CHECK {a} tellegen={c['tellegen']:.2e} "
                          f"reclose={c['reclose']:.2e} dv=[{c['dv_min']:.3e},"
                          f"{c['dv_max']:.3e}] unreached={c['n_unreached']}", flush=True)
            else:
                row, phi, nit, _why = S.solve_full(a, reuse, phi0=phi_prev)
                phi_prev = phi if args.warm else None
        except SolveRefused as e:
            nrefuse += 1
            print(f"{tag}REFUSED {a}: {e}", flush=True)
            continue
        store.put(i, row, nit, aux)
        times.append(time.time() - t)
        n = len(times)
        if n <= 3 or n % 100 == 0:
            el = time.time() - t0
            rem = (hi - lo - n) * np.median(times)
            print(f"{tag}[{el:7.1f}s] {n}/{hi-lo} {a} nit={nit} "
                  f"{times[-1]:.2f}s median={np.median(times):.2f}s "
                  f"eta={rem/60:.1f}min", flush=True)
            store.flush()
    store.flush()
    print(f"{tag}block {lo}:{hi} done, {len(times)} solved, "
          f"median {np.median(times) if times else float('nan'):.3f}s, "
          f"refused {nrefuse}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", choices=["medium", "kegg", "universe", "gpr"],
                    default="medium")
    ap.add_argument("--gpr-table", type=Path, default=None,
                    help="parquet with an `mnxr` column; its reaction set is the medium")
    ap.add_argument("--mode", choices=["full", "cone"], default="full")
    ap.add_argument("--cover", type=float, default=0.999)
    ap.add_argument("--leak", type=float, default=1e-6)
    ap.add_argument("--topk", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--limit", type=int, default=0, help="only the first N sources")
    ap.add_argument("--stride", type=int, default=0,
                    help="sample every Nth source instead of all (for timing)")
    ap.add_argument("--warm", action="store_true", default=True)
    ap.add_argument("--no-warm", dest="warm", action="store_false")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--channels", default="",
                    help="comma-separated extra per-reaction channels from the same solve: "
                         "power, dv. Off by default -- without it the store is byte-identical "
                         "to one written before channels existed.")
    ap.add_argument("--channel-check", action="store_true",
                    help="print Tellegen closure and the dv range on each worker's first "
                         "few solves")
    args = ap.parse_args()

    channels = tuple(c for c in args.channels.split(",") if c)
    bad = set(channels) - {"power", "dv"}
    if bad:
        raise SystemExit(f"unknown channels: {sorted(bad)}")
    if channels and args.mode == "cone":
        raise SystemExit("--channels needs --mode full")

    T0 = time.time()
    g, term = build(args.scale, args.gpr_table)
    print(f"[{time.time()-T0:.1f}s] {args.scale}: n={g.n} m={g.m} rxns={len(term)}",
          flush=True)
    S = GroundSystem(g, term, leak=args.leak)
    print(f"[{time.time()-T0:.1f}s] leaky n={S.n} m={S.m} n_rxn={S.n_rxn}", flush=True)

    names = list(S.rxn_names)
    order = proximity_order(S, [r for r in names if r in term])
    print(f"[{time.time()-T0:.1f}s] proximity order over {len(order)} sources", flush=True)
    if args.stride:
        order = order[::args.stride]
    if args.limit:
        order = order[:args.limit]

    F = SymmetricField(S)
    if args.mode == "cone":
        I, _ = S.injection(order[0])
        t = time.time()
        F.edge_abs(I)
        print(f"[{time.time()-T0:.1f}s] symmetric field ready [{time.time()-t:.1f}s] "
              f"{F.reuse.stats()}", flush=True)

    K = min(args.topk, S.n_rxn)
    prov = dict(scale=args.scale, element="C", leak=args.leak, mode=args.mode,
                warm=bool(args.warm), channels=list(channels),
                gpr_table=str(args.gpr_table) if args.gpr_table else None)
    fresh = [c for c in channels if not (args.out / f"{c}.f32").exists()]
    store = Store(args.out, order, S.rxn_names, K, provenance=prov, channels=channels)
    todo = int((store.done[:len(order)] == 0).sum())
    json.dump(dict(prov, argv=sys.argv, K=K, n_src=len(order)),
              open(args.out / "run.json", "w"), indent=1)
    if fresh and todo < len(order):
        raise SystemExit(f"channels {fresh} are new but {len(order)-todo} rows are already "
                         f"done in {args.out}; sweep to a fresh --out")
    print(f"[{time.time()-T0:.1f}s] store {args.out} K={K} todo={todo}/{len(order)}",
          flush=True)

    if args.workers <= 1:
        run_block(args, S, F, order, store, 0, len(order))
    else:
        bounds = np.linspace(0, len(order), args.workers + 1).astype(int)
        pids = []
        for w in range(args.workers):
            pid = os.fork()
            if pid == 0:
                st = Store(args.out, order, S.rxn_names, K, channels=channels)
                run_block(args, S, F, order, st, bounds[w], bounds[w + 1], tag=f"w{w} ")
                os._exit(0)
            pids.append(pid)
        for pid in pids:
            os.waitpid(pid, 0)

    store = Store(args.out, order, S.rxn_names, K)
    print(f"[{time.time()-T0:.1f}s] SWEEP_DONE {int(store.done.sum())}/{len(order)} rows",
          flush=True)


if __name__ == "__main__":
    main()
