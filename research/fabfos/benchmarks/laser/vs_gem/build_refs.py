#!/usr/bin/env python3
"""T1 -- the shared mapping layer every arm reads.

Emits, into vs_gem/refs/ and vs_gem/out/:

  refs/targets_resolved.tsv        every distinct target string -> status + MNXM(s)
  refs/carbon_resolved.tsv         every distinct carbon string -> MNXM(s)
  refs/edit_universe.tsv           the pooled add/del reaction universe
  refs/counterfactual_pool.parquet POOL_N seeded synthetic designs
  refs/design_index.tsv            one row per real design, with its strata
  out/panel_coverage.tsv           the coverage waterfall, arms as columns

Run:  python build_refs.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402
import resolve_names as RN  # noqa: E402

AA20 = [
    "L-alanine", "L-arginine", "L-asparagine", "L-aspartate", "L-cysteine",
    "L-glutamate", "L-glutamine", "glycine", "L-histidine", "L-isoleucine",
    "L-leucine", "L-lysine", "L-methionine", "L-phenylalanine", "L-proline",
    "L-serine", "L-threonine", "L-tryptophan", "L-tyrosine", "L-valine",
]

STATUS_VALUES = {"resolved", "biomass", "class_noun", "non_metabolite", "polymer",
                 "no_scoreable_element", "ambiguous", "unresolved",
                 "amino_acids", "not_a_carbon_source"}


def load_overrides(path: Path, tokens: set) -> tuple[dict, dict]:
    df = pd.read_csv(path, sep="\t", comment="#", dtype=str).fillna("")
    stale = sorted(set(df.token) - tokens)
    if stale:
        raise AssertionError(
            f"{path.name}: override rows whose token is no longer in the "
            f"extraction (stale table): {stale}")
    bad = sorted(set(df[df.kind == "status"].value) - STATUS_VALUES)
    if bad:
        raise AssertionError(f"{path.name}: unknown status value(s) {bad}")
    status = dict(zip(df[df.kind == "status"].token, df[df.kind == "status"].value))
    names = dict(zip(df[df.kind == "name"].token, df[df.kind == "name"].value))
    dup = set(status) & set(names)
    if dup:
        raise AssertionError(f"{path.name}: token declared twice {sorted(dup)}")
    return status, names


def resolve_column(r: RN.Resolver, tokens: dict, override_path: Path,
                   sep: str = ";") -> pd.DataFrame:
    """tokens: {token -> n_obs}. Returns the long resolution table."""
    status_ov, name_ov = load_overrides(override_path, set(tokens))
    rows = []
    for tok in sorted(tokens):
        n = tokens[tok]
        if tok in status_ov:
            rows.append(dict(token=tok, n_obs=n, status=status_ov[tok], tier="override",
                             note="status declaration", mnxms="", in_universe=False,
                             via=""))
            continue
        query = name_ov.get(tok, tok)
        res = r.resolve(query)
        if tok in name_ov and res["status"] != "resolved":
            raise AssertionError(
                f"{override_path.name}: name equivalence {tok!r} -> {query!r} no "
                f"longer resolves; the lexicon has rotted")
        rows.append(dict(
            token=tok, n_obs=n, status=res["status"],
            tier="override_name+" + res["tier"] if tok in name_ov else res["tier"],
            note=res["note"], mnxms=";".join(res["mnxms"]),
            in_universe=res["in_universe"], via=query if tok in name_ov else ""))
    return pd.DataFrame(rows)


def token_counts(series: pd.Series, sep: str) -> dict:
    out = {}
    for cell in series.dropna():
        for t in str(cell).split(sep):
            t = t.strip()
            if t:
                out[t] = out.get(t, 0) + 1
    return out


# ---------------------------------------------------------------------------

def build_edit_universe(s: pd.DataFrame, native: dict) -> pd.DataFrame:
    rows = {}
    for _, row in s.iterrows():
        for kind, rxns in (("add", row.add_rxns), ("del", row.del_rxns)):
            for rxn in rxns:
                k = (rxn, kind)
                d = rows.setdefault(k, dict(mnxr=rxn, kind=kind, n_conditions=0,
                                            native_k12=False, native_dh10b=False))
                d["n_conditions"] += 1
                d["native_k12"] = rxn in native["e_coli_k12"]
                d["native_dh10b"] = rxn in native["e_coli_dh10b"]
    return pd.DataFrame(sorted(rows.values(), key=lambda d: (d["kind"], d["mnxr"])))


SIZE_BINS = [(1, 1, "1"), (2, 2, "2"), (3, 4, "3-4"), (5, 8, "5-8"),
             (9, 16, "9-16"), (17, 10 ** 9, "17+")]


def size_bin(total: int) -> str:
    if total < 1:
        return "0"
    for lo, hi, lab in SIZE_BINS:
        if lo <= total <= hi:
            return lab
    return "17+"


def composition(n_add: int, n_del: int) -> str:
    if n_add and n_del:
        return "mixed"
    if n_add:
        return "add_only"
    if n_del:
        return "del_only"
    return "empty"


def build_pool(s: pd.DataFrame, add_universe: list, del_universe: list,
               n: int = C.POOL_N, seed: int = C.POOL_SEED) -> pd.DataFrame:
    """Counterfactual designs: sizes drawn from the empirical joint (n_add, n_del)
    of the real designs, reactions drawn uniformly without replacement from the
    pooled LASER universe. Seeded once and shared by every arm and by FBA -- if
    two arms see different designs their p-values are not comparable."""
    rng = np.random.default_rng(seed)
    sizes = list(zip(s.add_rxns.apply(len), s.del_rxns.apply(len)))
    picks = rng.integers(0, len(sizes), size=n)
    add_u = np.array(sorted(add_universe))
    del_u = np.array(sorted(del_universe))
    rows = []
    for i, k in enumerate(picks):
        na, nd = sizes[int(k)]
        na, nd = min(na, len(add_u)), min(nd, len(del_u))
        a = rng.choice(add_u, size=na, replace=False) if na else np.array([], str)
        d = rng.choice(del_u, size=nd, replace=False) if nd else np.array([], str)
        rows.append(dict(
            design_id=f"CF{i:04d}", is_counterfactual=True,
            donor_condition=s.obs_id.iloc[int(k)],
            add_mnxr=",".join(sorted(a)), del_mnxr=",".join(sorted(d)),
            n_add=int(na), n_del=int(nd), total_edits=int(na + nd),
            size_bin=size_bin(int(na + nd)), composition=composition(int(na), int(nd))))
    return pd.DataFrame(rows)


def build_design_index(s: pd.DataFrame, native: dict, tgt: pd.DataFrame) -> pd.DataFrame:
    tstat = dict(zip(tgt.token, tgt.status))
    tmnx = dict(zip(tgt.token, tgt.mnxms))
    tuni = dict(zip(tgt.token, tgt.in_universe))
    rows = []
    for _, row in s.iterrows():
        nat = native.get(row.host_dir, set())
        het = [x for x in row.add_rxns if x not in nat]
        applied_del = [x for x in row.del_rxns if x in nat]
        toks = [t.strip() for t in str(row.target).split(";") if t.strip()]
        res = [t for t in toks if tstat.get(t) == "resolved"]
        rows.append(dict(
            design_id=row.obs_id, obs_id=row.obs_id, condition_id=row.obs_id,
            is_counterfactual=False, source_record=row.source_record,
            host_gem=row.host_gem, host_dir=row.host_dir,
            target=row.target, target_tokens=";".join(toks),
            target_status=";".join(sorted({tstat.get(t, "unresolved") for t in toks})),
            target_mnxms=";".join(sorted({m for t in res for m in tmnx[t].split(";") if m})),
            target_in_universe=any(bool(tuni.get(t)) for t in res),
            is_biomass=any(tstat.get(t) == "biomass" for t in toks),
            measured=row.measured, magnitude=row.magnitude,
            measurement_type=row.measurement_type,
            medium=row.medium, carbon=row.carbon, is_probe=bool(row.is_probe),
            add_mnxr=",".join(row.add_rxns), del_mnxr=",".join(row.del_rxns),
            n_add=len(row.add_rxns), n_del=len(row.del_rxns),
            total_edits=len(row.add_rxns) + len(row.del_rxns),
            size_bin=size_bin(len(row.add_rxns) + len(row.del_rxns)),
            composition=composition(len(row.add_rxns), len(row.del_rxns)),
            n_heterologous_add=len(het),
            no_heterologous_add=len(het) == 0,
            deletion_only=len(row.add_rxns) == 0,
            all_native_add=len(row.add_rxns) > 0 and len(het) == 0,
            topology_unchanged=len(het) == 0 and len(applied_del) == 0,
        ))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------

def main():
    df = C.load_extraction()
    native = {h: C.host_native_reactions(h) for h in ("e_coli_k12", "e_coli_dh10b")}
    census = C.assert_census(df, native)
    s = C.scored_set(df)
    print(json.dumps(census, indent=1))

    r = RN.Resolver("C")

    tgt_tokens = token_counts(s.target, ";")
    tgt = resolve_column(r, tgt_tokens, C.REFS / "target_overrides.tsv")
    tgt.to_csv(C.REFS / "targets_resolved.tsv", sep="\t", index=False)

    car_tokens = token_counts(s.carbon, ",")
    car = resolve_column(r, car_tokens, C.REFS / "carbon_overrides.tsv")
    aa = [r.resolve(a) for a in AA20]
    aa_mnxm = sorted({m for x in aa for m in x["mnxms"]})
    missing_aa = [a for a, x in zip(AA20, aa) if not x["mnxms"]]
    if missing_aa:
        raise AssertionError(f"amino acid terminal incomplete: {missing_aa}")
    car.loc[car.status == "amino_acids", "mnxms"] = ";".join(aa_mnxm)
    car.loc[car.status == "amino_acids", "in_universe"] = True
    car.to_csv(C.REFS / "carbon_resolved.tsv", sep="\t", index=False)
    (C.REFS / "amino_acid_terminal.json").write_text(
        json.dumps({"names": AA20, "mnxms": aa_mnxm}, indent=1))

    eu = build_edit_universe(s, native)
    eu.to_csv(C.REFS / "edit_universe.tsv", sep="\t", index=False)
    add_u = sorted(eu[eu.kind == "add"].mnxr)
    del_u = sorted(eu[eu.kind == "del"].mnxr)
    assert len(add_u) == 472 and len(del_u) == 222, (len(add_u), len(del_u))

    pool = build_pool(s, add_u, del_u)
    pool.to_parquet(C.REFS / "counterfactual_pool.parquet", index=False)

    idx = build_design_index(s, native, tgt)
    idx.to_csv(C.REFS / "design_index.tsv", sep="\t", index=False)
    if int(idx.topology_unchanged.sum()) != 30:
        raise AssertionError(
            f"topology_unchanged drifted: {int(idx.topology_unchanged.sum())} want 30")

    # -- coverage waterfall ------------------------------------------------
    universe = r.universe
    rows = []

    def gate(name, keep_obs, keep_tok, lost_example=""):
        rows.append(dict(gate=name, n_obs=keep_obs, n_targets=keep_tok,
                         example_lost=lost_example))

    gate("raw", len(df), df.target.nunique())
    gate("mutation filter (147 lost)", len(s), s.target.nunique(),
         "; ".join(sorted(df[df.has_mutation].target.dropna().unique())[:2]))
    nonprobe = idx[~idx.is_probe]
    gate("non-probe (medium or carbon named)", len(nonprobe), nonprobe.target.nunique(),
         "curated:* mechanism probes")
    named = nonprobe[nonprobe.target_status.str.contains("resolved")]
    dropped = sorted(set(nonprobe.target) - set(named.target))
    gate("names a specific product", len(named), named.target.nunique(),
         "; ".join(dropped[:3]))
    inuni = named[named.target_in_universe]
    gate("target in the C atom universe", len(inuni), inuni.target.nunique(),
         "; ".join(sorted(set(named.target) - set(inuni.target))[:3]))
    cov = pd.DataFrame(rows)
    cov.to_csv(C.OUT / "panel_coverage_stage1.tsv", sep="\t", index=False)

    print("\n== target resolution ==")
    print(tgt.groupby("status").agg(tokens=("token", "size"),
                                    obs=("n_obs", "sum")).to_string())
    print("\n== coverage (graph-independent gates) ==")
    print(cov.to_string(index=False))
    print("\n== strata on the 235 ==")
    for c in ("no_heterologous_add", "deletion_only", "all_native_add",
              "topology_unchanged", "is_probe", "is_biomass", "target_in_universe"):
        print(f"  {c:24s} {int(idx[c].sum())}")
    print("\n== counterfactual pool ==")
    print(pool.groupby(["size_bin", "composition"]).size().to_string())
    print(f"\nwrote refs/ and out/ under {C.HERE}")


if __name__ == "__main__":
    main()
