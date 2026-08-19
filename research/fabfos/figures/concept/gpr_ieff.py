import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.sparse as sp

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from atom_graph import build_atom_graph, restrict_to_giant             # noqa: E402
from ieff_ground import GroundSystem, edge_current, newton_rhs         # noqa: E402
from ieff_sweep import gpr_medium                                      # noqa: E402

LEVELS = (0.5, 0.9, 0.99, 0.999, 0.9999)


def frac_at(sorted_desc, levels=LEVELS):
    total = float(sorted_desc.sum())
    if total <= 0:
        return {str(l): dict(n=0, frac=0.0) for l in levels}
    c = np.cumsum(sorted_desc) / total
    n = len(sorted_desc)
    return {str(l): dict(n=int(np.searchsorted(c, l) + 1),
                         frac=float((np.searchsorted(c, l) + 1) / n)) for l in levels}


def cmd_probe(args):
    T0 = time.time()
    g, term, met_sym = build_atom_graph(args.element, gpr_medium(args.gpr_table))
    g, term = restrict_to_giant(g, term)
    S = GroundSystem(g, term, leak=args.leak)
    print(f"[{time.time()-T0:.1f}s] {args.gpr_table.name}: giant n={g.n} m={g.m} "
          f"rxns={len(term)}; leaky n={S.n} m={S.m}", flush=True)

    code_of = {s: c for c, s in met_sym.items()}
    if args.met not in code_of:
        raise SystemExit(f"{args.met} is not in the bake vocabulary")
    code = code_of[args.met]
    src = [i for i in range(g.n) if g.nodes[i][0] == code]
    if not src:
        raise SystemExit(f"{args.met} has no {args.element} atom node in this host's giant "
                         "component -- it is not reachable by this gene set")
    print(f"source {args.met}: {len(src)} {args.element} atom nodes "
          f"(ranks {sorted(g.nodes[i][1] for i in src)})", flush=True)

    I, src = S.injection_at(src)
    reuse = S.full_reuse()
    phi, nit, why = newton_rhs(S.B, S.gp, S.gm, I, S.keep, reuse)
    ie = edge_current(S.B, S.gp, S.gm, phi)
    ie_abs = np.abs(ie)
    print(f"[{time.time()-T0:.1f}s] solved: {nit} Newton iterations ({why}), "
          f"{reuse.stats()}", flush=True)

    into_ground = float(-(S.B.T @ ie)[S.ground])
    kcl = float(np.abs((S.B.T @ ie) - I)[S.keep].max())
    print(f"conservation: {into_ground:.12f} A into ground, KCL residual {kcl:.2e}",
          flush=True)

    thr = S.node_throughput(ie_abs)
    met_codes = np.full(S.n, -1, np.int64)
    for i, nd in enumerate(S.gl.nodes):
        if i != S.ground:
            met_codes[i] = int(nd[0])
    uc, inv = np.unique(met_codes, return_inverse=True)
    met_cur = np.bincount(inv, thr, minlength=len(uc))
    keep = uc >= 0
    uc, met_cur = uc[keep], met_cur[keep]
    met_names = np.array([met_sym[c] for c in uc], dtype=object)

    leak_tail = S.E[S.m_rxn:, 0]
    drain = np.bincount(met_codes[leak_tail], ie_abs[S.m_rxn:],
                        minlength=int(met_codes.max()) + 1)
    met_drain = drain[uc]

    rxn_cur = S.attribute(ie_abs)
    edge_cur = ie_abs[:S.m_rxn]

    out = dict(
        host_table=str(args.gpr_table), element=args.element, source=args.met,
        leak=args.leak, n_source_atoms=len(src),
        graph=dict(n_atom_nodes=int(g.n), n_edges=int(g.m), n_reactions=int(len(term)),
                   n_metabolites=int(len(uc))),
        solve=dict(newton_iterations=int(nit), stop=why,
                   current_into_ground=into_ground, kcl_residual=kcl,
                   worst_relative_residual=float(reuse.worst_rel)),
        cumulative=dict(
            metabolites_transit=frac_at(np.sort(met_cur)[::-1]),
            metabolites_drain=frac_at(np.sort(met_drain)[::-1]),
            reactions=frac_at(np.sort(rxn_cur)[::-1]),
            edges=frac_at(np.sort(edge_cur)[::-1]),
        ),
        totals=dict(metabolite_transit=float(met_cur.sum()),
                    metabolite_drain=float(met_drain.sum()),
                    reaction_transit=float(rxn_cur.sum()),
                    edge_transit=float(edge_cur.sum())),
    )
    o = np.argsort(met_cur)[::-1][:args.top]
    out["top_metabolites"] = [dict(mnxm=str(met_names[i]), transit=float(met_cur[i]),
                                   drain=float(met_drain[i])) for i in o]
    o = np.argsort(met_drain)[::-1][:args.top]
    out["top_drains"] = [dict(mnxm=str(met_names[i]), drain=float(met_drain[i]),
                              transit=float(met_cur[i])) for i in o]
    o = np.argsort(rxn_cur)[::-1][:args.top]
    out["top_reactions"] = [dict(mnxr=str(S.rxn_names[i]), current=float(rxn_cur[i]))
                            for i in o]

    for what in ("metabolites_transit", "metabolites_drain", "reactions", "edges"):
        c = out["cumulative"][what]
        line = "  ".join(f"{l}: {c[str(l)]['n']} ({c[str(l)]['frac']*100:.1f}%)"
                         for l in LEVELS)
        print(f"{what:>20}  {line}", flush=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    json.dump(out, open(args.out, "w"), indent=2)
    print(f"wrote {args.out}", flush=True)

    if args.tsv:
        pd.DataFrame(dict(mnxm=met_names, transit=met_cur, drain=met_drain)).sort_values(
            "drain", ascending=False).to_csv(args.tsv, sep="\t", index=False)
        print(f"wrote {args.tsv}", flush=True)


def cmd_pack(args):
    meta = json.load(open(args.store / "meta.json"))
    src, rxn, K = meta["src"], meta["rxn"], meta["K"]
    n = len(src)
    idx = np.memmap(args.store / "idx.i32", np.int32, "r", shape=(n, K))
    val = np.memmap(args.store / "val.f32", np.float32, "r", shape=(n, K))
    done = np.asarray(np.memmap(args.store / "done.u8", np.uint8, "r", shape=(n,)))
    rowsum = np.asarray(np.memmap(args.store / "rowsum.f64", np.float64, "r", shape=(n,)))
    if not done.all():
        raise SystemExit(f"store is incomplete: {int(done.sum())}/{n} rows")

    rows, cols, vals, kept = [], [], [], np.zeros(n, np.int32)
    covered = np.zeros(n)
    for i in range(n):
        o = np.argsort(val[i])[::-1]
        v = val[i][o].astype(np.float64)
        col = idx[i][o]
        tot = rowsum[i]
        c = np.cumsum(v)
        if tot <= 0:
            continue
        k = int(np.searchsorted(c, args.cover * tot) + 1)
        k = min(k, K)
        kept[i] = k
        covered[i] = c[k - 1] / tot
        rows.append(np.full(k, i, np.int32))
        cols.append(col[:k])
        vals.append(v[:k])

    rows = np.concatenate(rows) if rows else np.zeros(0, np.int32)
    cols = np.concatenate(cols) if cols else np.zeros(0, np.int32)
    vals = np.concatenate(vals) if vals else np.zeros(0)
    A = sp.csr_matrix((vals, (rows, cols)), shape=(n, len(rxn)))

    dens = A.nnz / (n * len(rxn))
    print(f"kept per row: median {int(np.median(kept))} of {len(rxn)} targets "
          f"(min {int(kept.min())}, max {int(kept.max())}); "
          f"nnz={A.nnz} density={dens:.4f}", flush=True)
    print(f"realised cover: min {covered.min():.4f} median {np.median(covered):.4f}",
          flush=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    if args.dense_out:
        D = np.zeros((n, len(rxn)), np.float32)
        for i in range(n):
            D[i, idx[i]] = val[i]
        np.save(args.dense_out, D)
        print(f"wrote {args.dense_out} ({D.nbytes/1e6:.1f} MB dense)", flush=True)

    np.savez_compressed(args.out, data=A.data.astype(np.float32), indices=A.indices,
                        indptr=A.indptr, shape=np.array(A.shape),
                        rowsum=rowsum, src=np.array(src), rxn=np.array(rxn))
    prov = {k: meta[k] for k in ("scale", "element", "leak", "mode", "warm", "gpr_table")
            if k in meta}
    if args.gpr_table:
        prov["gpr_table"] = str(args.gpr_table)
        prov.setdefault("element", "C")
    if prov.get("gpr_table"):
        t = Path(prov["gpr_table"])
        d = pd.read_parquet(t, columns=["host", "build_id"])
        prov["host"] = sorted(d.host.dropna().unique().tolist())
        prov["build_id"] = sorted(d.build_id.dropna().unique().tolist())

    info = dict(
        source=prov,
        store=str(args.store), cover=args.cover, shape=list(A.shape), nnz=int(A.nnz),
        density=float(dens), source_reactions=n, target_reactions=len(rxn),
        kept_per_row=dict(min=int(kept.min()), median=float(np.median(kept)),
                          max=int(kept.max()), mean=float(kept.mean())),
        realised_cover=dict(min=float(covered.min()), median=float(np.median(covered))),
        rowsum=dict(min=float(rowsum.min()), median=float(np.median(rowsum)),
                    max=float(rowsum.max())),
        semantics=("entry (a,b) = current reaction b draws when 1 A is injected at "
                   "reaction a's product atoms and drained at the OMEGA universal leak, "
                   "solved exactly on the rectified atom-resolved graph. Asymmetric. "
                   "`rowsum` is the untruncated row total, which is transit, not a "
                   "conserved quantity."),
    )
    json.dump(info, open(args.out.with_suffix(".meta.json"), "w"), indent=2)
    print(f"wrote {args.out} and {args.out.with_suffix('.meta.json')}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("probe", help="one metabolite source, one solve, sparsity report")
    p.add_argument("--gpr-table", type=Path, required=True)
    p.add_argument("--met", default="MNXM1364061", help="MetaNetX id (default D-glucose)")
    p.add_argument("--element", default="C")
    p.add_argument("--leak", type=float, default=1e-6)
    p.add_argument("--top", type=int, default=40)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--tsv", type=Path, default=None, help="per-metabolite current table")
    p.set_defaults(fn=cmd_probe)

    p = sub.add_parser("pack", help="sweep store -> sparse matrix at a current cover")
    p.add_argument("--store", type=Path, required=True)
    p.add_argument("--cover", type=float, default=0.99)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--dense-out", type=Path, default=None,
                   help="also write the untruncated matrix, which ieff_validate needs")
    p.add_argument("--gpr-table", type=Path, default=None,
                   help="record the host reaction set in the output meta; only needed for a "
                        "store written before the sweep recorded it itself")
    p.set_defaults(fn=cmd_pack)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
