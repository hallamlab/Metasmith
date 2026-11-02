import argparse
import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import os                                                            # noqa: E402
SRC = os.environ.get("ECSPR_SRC", str(HERE.parents[3] / "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)


def overlap(A, B, k):
    out = []
    for i in range(len(A)):
        a = np.argsort(A[i])[::-1][:k]
        b = np.argsort(B[i])[::-1][:k]
        out.append(len(set(a) & set(b)) / k)
    return float(np.mean(out))


def conservation(scale, n=6, leak=1e-6, gpr_table=None):
    import pandas as pd
    from ieff_ground import GroundSystem, edge_current, newton_rhs
    from ieff_sweep import build

    g, term = build(scale, gpr_table)
    S = GroundSystem(g, term, leak=leak)
    reuse = S.full_reuse()
    rng = np.random.default_rng(2)
    names = sorted(term)
    prov = S.gl.meta["edge_reactions"]
    rows = []
    for a in [names[i] for i in rng.choice(len(names), n, replace=False)]:
        I, _src = S.injection(a)
        phi, _nit, _why = newton_rhs(S.B, S.gp, S.gm, I, S.keep, reuse)
        ie = edge_current(S.B, S.gp, S.gm, phi)
        leak_out = float(np.abs(ie[S.m_rxn:]).sum())
        kcl = float(np.abs((S.B.T @ ie - I)[S.keep]).max())

        mine = S.attribute(np.abs(ie))
        lib = (pd.Series(np.abs(ie)[S.pe] * S.pfrac, index=prov.mnxr.to_numpy())
               .groupby(level=0).sum())
        lv = np.array([float(lib[r]) for r in S.rxn_names])
        rel = float(np.abs(mine - lv).max() / max(lv.max(), 1e-30))
        rows.append(dict(src=a, leak_out=leak_out, kcl=kcl,
                         attribution_max_rel_diff=rel))
        print(f"  {a}: current into OMEGA = {leak_out:.9f} A (1.0 injected), "
              f"KCL residual {kcl:.2e}, attribution vs reaction_currents formula "
              f"max rel diff {rel:.2e}", flush=True)
    return rows


def audit_store(store):
    from ieff_layout import load_store
    st = load_store(store)
    done = np.asarray(st["done"]).astype(bool)
    rs = np.asarray(st["rowsum"])[done]
    val = np.asarray(st["val"])[done].astype(np.float64)
    kept = val.sum(1) / np.maximum(rs, 1e-30)
    empty = int((rs <= 0).sum())
    q = lambda a, p: float(np.percentile(a, p))                        # noqa: E731
    out = dict(rows=int(st["n"]), done=int(done.sum()), K=int(st["K"]),
               targets=len(st["meta"]["rxn"]), empty_rows=empty,
               rowsum_min=float(rs.min()), rowsum_median=float(np.median(rs)),
               rowsum_max=float(rs.max()),
               retained_p1=q(kept, 1), retained_median=q(kept, 50),
               retained_min=float(kept.min()))
    print(f"  rows {out['done']}/{out['rows']} solved, K={out['K']} of "
          f"{out['targets']} targets, {empty} empty", flush=True)
    print(f"  row total attributed current: min {out['rowsum_min']:.6f} median "
          f"{out['rowsum_median']:.6f} max {out['rowsum_max']:.6f} A "
          f"(1.0 injected per source)", flush=True)
    print(f"  fraction of row mass inside the stored top-K: median "
          f"{out['retained_median']:.4f}, p1 {out['retained_p1']:.4f}, "
          f"min {out['retained_min']:.4f}", flush=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", type=Path, required=True)
    ap.add_argument("--dense", type=Path, default=None,
                    help="dense.npy written beside the store (medium scale only); without "
                         "it only conservation and the store audit run")
    ap.add_argument("--scale", default="medium")
    ap.add_argument("--gpr-table", type=Path, default=None,
                    help="required by --scale gpr; the host reaction set to re-solve on")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--skip-conservation", action="store_true")
    args = ap.parse_args()

    from ieff_layout import pathway_members, PANEL, scatter_index
    import umap

    res = {}
    if not args.skip_conservation:
        print("conservation:", flush=True)
        res["conservation"] = conservation(args.scale, gpr_table=args.gpr_table)

    print("\nstore audit:", flush=True)
    res["store"] = audit_store(args.store)

    if args.dense is None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        json.dump(res, open(args.out, "w"), indent=1)
        print(f"\nwrote {args.out} (no --dense: exact-vs-landmark and min_K skipped)",
              flush=True)
        return

    meta = json.load(open(args.store / "meta.json"))
    E = np.load(args.dense).astype(np.float64)
    src = meta["src"]
    n = len(src)
    Esym = np.maximum(E, E.T)
    np.fill_diagonal(Esym, 0.0)
    print(f"\nexact matrix {E.shape}; symmetrised", flush=True)

    rng = np.random.default_rng(0)
    members, names = pathway_members()
    lab = {}
    for pid in PANEL:
        for r in members.get(pid, ()):
            lab.setdefault(r, pid)
    labels = np.array([lab.get(r, "") for r in src])

    def embed_exact(k=30, seed=42):
        d = np.log10(Esym.max() / np.maximum(Esym, 1e-30))
        np.fill_diagonal(d, 0.0)
        return umap.UMAP(n_components=2, metric="precomputed", n_neighbors=k,
                         min_dist=0.05, init="random", random_state=seed).fit_transform(d)

    def embed_landmark(cols, k=30, seed=42):
        F = np.log10(E[cols].T + 1e-12)
        return umap.UMAP(n_components=2, metric="cosine", n_neighbors=k,
                         min_dist=0.05, init="random", random_state=seed).fit_transform(F)

    def landmark_sim(cols):
        F = np.log10(E[cols].T + 1e-12)
        F = F - F.mean(1, keepdims=True)
        Fn = F / np.maximum(np.linalg.norm(F, axis=1, keepdims=True), 1e-30)
        Sm = Fn @ Fn.T
        np.fill_diagonal(Sm, -np.inf)
        return Sm

    allc = np.arange(n)
    curves = []
    for K in [16, 32, 64, 128, 296, 592, n]:
        cols = allc if K >= n else np.sort(rng.choice(n, K, replace=False))
        Sm = landmark_sim(cols)
        row = dict(K=int(len(cols)),
                   **{f"top{k}": overlap(Esym, Sm, k) for k in (5, 10, 30)})
        curves.append(row)
        print(f"  landmark K={row['K']:<5} vs exact: top5={row['top5']:.3f} "
              f"top10={row['top10']:.3f} top30={row['top30']:.3f}", flush=True)
    res["min_k_landmarks"] = curves

    mass = []
    order = np.argsort(Esym, axis=1)[:, ::-1]
    tot = Esym.sum(1, keepdims=True)
    cum = np.take_along_axis(Esym, order, 1).cumsum(1) / np.maximum(tot, 1e-30)
    for q in (0.5, 0.8, 0.9, 0.95, 0.99, 0.999):
        kq = np.median((cum < q).sum(1) + 1)
        T = np.zeros_like(Esym)
        keepmask = cum <= q
        keepmask[:, 0] = True
        np.put_along_axis(T, order, np.where(keepmask,
                                             np.take_along_axis(Esym, order, 1), 0.0), 1)
        mass.append(dict(mass=q, median_partners=float(kq),
                         top10=overlap(Esym, T, 10), top30=overlap(Esym, T, 30)))
        print(f"  retain {q*100:5.1f}% of row current mass -> median "
              f"{kq:.0f} partners, top10={mass[-1]['top10']:.3f} "
              f"top30={mass[-1]['top30']:.3f}", flush=True)
    res["min_k_mass"] = mass

    xy_e = embed_exact()
    xy_l = embed_landmark(np.sort(rng.choice(n, min(296, n), replace=False)))
    rng2 = np.random.default_rng(7)
    si = {}
    for pid in PANEL:
        ii = np.where(labels == pid)[0]
        if len(ii) > 2:
            si[pid] = dict(n=int(len(ii)), name=names[pid],
                           exact=float(scatter_index(xy_e, ii, rng2)),
                           landmark=float(scatter_index(xy_l, ii, rng2)))
            print(f"  {pid} {names[pid][:32]:<34} n={si[pid]['n']:<4} "
                  f"exact={si[pid]['exact']:.3f}  landmark={si[pid]['landmark']:.3f}",
                  flush=True)
    res["scatter_index"] = si
    res["scatter_index_mean"] = dict(
        exact=float(np.mean([v["exact"] for v in si.values()])),
        landmark=float(np.mean([v["landmark"] for v in si.values()])))
    print(f"  MEAN exact={res['scatter_index_mean']['exact']:.3f}  "
          f"landmark={res['scatter_index_mean']['landmark']:.3f}", flush=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    json.dump(res, open(args.out, "w"), indent=1)
    print(f"\nwrote {args.out}", flush=True)


if __name__ == "__main__":
    main()
