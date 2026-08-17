"""How much of the direction gap is rescuable, per mechanism, measured.

The r7 bake leaves 47,266 of 83,795 reactions at `dir_tier=0` -- no member spoke,
so the ratio defaults to 1.0, and "no evidence" is indistinguishable downstream
from "genuinely reversible". This script partitions that gap by the machinery that
would have to change for each part of it to move, so a reader can price the repairs
against each other instead of against a single aggregate.

EVERY ROW IS A SET OPERATION over three committed artifacts and nothing else: the
deployed direction annotation, the two `ecspr.bake.direction.forecast` tables (one
built with `--mnxm-only` to reproduce the pre-fix loader, one without), and the
bake's atom_pairs. No member is run. That is what makes the numbers cheap enough to
re-take rather than transcribe -- the r7 lesson is that a reading left as prose is
most of the cost of redoing it.

TWO DENOMINATORS, ALWAYS. A reaction with no atom pairs carries no graph edge, so
rescuing its direction changes no conductance. `in_graph` is the number that
matters to a consumer; the bare count is the number that matters to the ensemble.

Reproduce:
    R=data/fabfos/originals/metanetx/4.5
    W=<scratch>
    PYTHONPATH=src mamba run -n rdkit-scratch python -m ecspr.bake.direction.forecast build \\
        --reac-prop $R/reac_prop.tsv --chem-prop $R/chem_prop.tsv --mnxm-only \\
        --out $W/forecast_asdeployed.parquet --out-summary $W/summary_asdeployed.tsv
    PYTHONPATH=src mamba run -n rdkit-scratch python -m ecspr.bake.direction.forecast build \\
        --reac-prop $R/reac_prop.tsv --chem-prop $R/chem_prop.tsv \\
        [--resolution $W/resolution.parquet] \\
        --out $W/forecast_postfix.parquet --out-summary $W/summary_postfix.tsv
    PYTHONPATH=src mamba run -n rdkit-scratch python \\
        research/fabfos/benchmarks/direction_rescue/measure_rescue.py --work $W
"""
from __future__ import annotations

import argparse
import collections
import re
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
MNX = REPO / "data" / "fabfos" / "originals" / "metanetx" / "4.5"
BAKE = REPO / "data" / "fabfos" / "processed" / "metabolism_bake"

# The generic-carrier vocabulary, as a name predicate over chem_prop's own names.
#
# MetaNetX underspecifies these two ways -- a `*`-bearing SMILES or no SMILES at
# all -- and the two populations are the SAME COMPOUNDS, which is why the two
# rescue rows below share this one predicate. A name rule rather than a curated
# accession list because MetaNetX files the same carrier under many accessions
# (`AH2` is at least MNXM1102421 and MNXM1105763), so an accession list would be a
# list of the ones somebody happened to look at.
CARRIER = re.compile(
    r"(acceptor|donor|\[|^ACP$|carrier|ferredoxin|cytochrome|flavodoxin|thioredoxin"
    r"|glutaredoxin|^AH2$|^A$|^Unknown$|^R$|^RH$|protein|oxidized|reduced|electron)",
    re.I)


def load_names():
    out = {}
    with open(MNX / "chem_prop.tsv") as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            p = line.split("\t")
            if len(p) > 1:
                out[p[0]] = p[1]
    return out


def load_mapped():
    """Reactions carrying at least one atom pair -- the in-graph denominator."""
    from ecspr.bake import encoding as refs
    V = refs.load_vocab(BAKE / "vocab.parquet")
    rxn = V.df[V.df["kind"] == "rxn"]
    sym = dict(zip(rxn["code"], rxn["symbol"]))
    codes = pd.read_parquet(BAKE / "atom_pairs.parquet", columns=["rxn"])["rxn"].unique()
    return {sym[c] for c in codes}


def spoke(fc, member=None):
    """Reactions the forecast expects at least one member (or `member`) to answer."""
    d = fc if member is None else fc[fc["member"] == member]
    return set(d.loc[d["mechanism"] == "expected_ok", "mnxr"])


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", required=True, type=Path,
                    help="directory holding forecast_asdeployed.parquet and "
                         "forecast_postfix.parquet")
    ap.add_argument("--out", type=Path, default=None, help="write the table as TSV")
    a = ap.parse_args(argv)

    from ecspr.bake.direction.refdata import load_mnxr_stoich, load_mnxm_props
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")

    pre = pd.read_parquet(a.work / "forecast_asdeployed.parquet")
    post = pd.read_parquet(a.work / "forecast_postfix.parquet")
    ann = pd.read_parquet(BAKE / "seams" / "direction_annotation.parquet",
                          columns=["mnxr", "dir_tier"])
    mapped = load_mapped()
    names = load_names()
    stoich = load_mnxr_stoich(MNX / "reac_prop.tsv")
    props = load_mnxm_props(MNX / "chem_prop.tsv")

    tier = {t: set(ann.loc[ann["dir_tier"] == t, "mnxr"]) for t in (0, 1, 2, 3)}
    post_silent = set(ann["mnxr"]) - spoke(post)
    still = tier[0] & post_silent          # tier 0 that the water fix does NOT move
    print(f"universe {len(ann):,} · tier 0 {len(tier[0]):,} "
          f"(in-graph {len(tier[0] & mapped):,}) · tier 3 {len(tier[3]):,}")
    print(f"tier 0 both members STILL silent after the water fix: {len(still):,} "
          f"(in-graph {len(still & mapped):,})")

    rows = []

    def row(key, s, confidence, note):
        rows.append((key, len(s), len(s & mapped), confidence, note))

    # ---- 1/2: the water fix, per member and per tier it lands in ----------
    gained_db = spoke(post, "dgbyg") - spoke(pre, "dgbyg")
    gained_eq = spoke(post, "eq") - spoke(pre, "eq")
    row("water_fix.tier0.dgbyg", gained_db & tier[0], "high",
        "dGbyG answered 25,053/25,053 of the balanced reactions that reached it in r7")
    row("water_fix.tier0.eq", gained_eq & tier[0], "high" if "resolved" else "high",
        "eQuilibrator; upper bound unless the forecast was given --resolution")
    row("water_fix.tier0.either", (gained_db | gained_eq) & tier[0], "high",
        "the reaction leaves tier 0 if EITHER member speaks")
    row("water_fix.tier3.either", (gained_db | gained_eq) & tier[3], "high",
        "curated-only rows that gain a thermo vote and so change tier and ratio")

    # ---- 3/5: the carriers, which are ONE population wearing two hats -----
    # A reaction whose every unreadable participant is a generic carrier is one a
    # carrier table would complete. Splitting it by whether the REST of the equation
    # balances is what separates "a lookup finishes this" from "a lookup plus a
    # rebalance might".
    def unreadable(s):
        return [m for m in s if (props.get(m) or {}).get("smiles") is None]

    def wildcards(s):
        out = []
        for m in s:
            smi = (props.get(m) or {}).get("smiles")
            if smi and "*" in smi:
                out.append(m)
        return out

    def heavy(m):
        smi = (props.get(m) or {}).get("smiles")
        if not smi:
            return None
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            return None
        return collections.Counter(a.GetSymbol() for a in Chem.AddHs(mol).GetAtoms()
                                   if a.GetSymbol() != "H")

    heavy_cache = {}

    def heavy_of(m):
        if m not in heavy_cache:
            heavy_cache[m] = heavy(m)
        return heavy_cache[m]

    def remainder_balances(s, drop):
        """Do the heavy atoms balance once `drop`'s participants are removed?

        The claim a carrier table makes: the carrier pair contributes a tabulated
        dE'0 and everything else is ordinary chemistry. It is only a lookup if the
        rest of the equation is already closed.
        """
        tot = collections.Counter()
        for m, coeff in s.items():
            if m in drop:
                continue
            h = heavy_of(m)
            if h is None:
                return False
            for el, n in h.items():
                tot[el] += coeff * n
        return all(abs(v) < 1e-9 for v in tot.values())

    carrier_only, carrier_closed = set(), set()
    wildcard_only, wildcard_closed = set(), set()
    for mnxr in still:
        s = stoich[mnxr][0]
        bad = unreadable(s)
        if bad and all(CARRIER.search(names.get(m, "") or "") for m in bad):
            carrier_only.add(mnxr)
            if remainder_balances(s, set(bad)):
                carrier_closed.add(mnxr)
        wc = wildcards(s)
        if wc and not bad:
            wildcard_only.add(mnxr)
            if remainder_balances(s, set(wc)):
                wildcard_closed.add(mnxr)

    row("carrier.closed_remainder", carrier_closed, "medium",
        "every unreadable participant is a generic carrier AND the rest of the "
        "equation balances -- the direction is a dE'0 lookup, not a model. Needs a "
        "curated E'0 table, which does not exist yet")
    row("carrier.crosswalk_ceiling", carrier_only, "low",
        "every unreadable participant is a generic carrier, balance unknown. Needs "
        "structures AND a rebalance; the stretch case")
    row("wildcard.closed_remainder", wildcard_closed, "low-medium",
        "the only blockers are R-group residues and the body balances without them "
        "-- capping the residues is structurally sound and unvalidated")
    row("wildcard.ceiling", wildcard_only, "low",
        "R-group residues are the only blocker, remainder not necessarily closed")

    # ---- 6: unbalanced, with nothing else in the way ---------------------
    unb = set(post.loc[(post["member"] == "dgbyg")
                       & (post["mechanism"] == "unbalanced"), "mnxr"]) & still
    row("unbalanced.only_blocker", unb, "very low",
        "dG'0 of an unbalanced equation is not a physical quantity; listed so the "
        "residue is accounted for, not because it is a lever")

    # ---- 7: the element-neutrality predicate, re-measured as a dead end ---
    # `aam_blockers` rescues a structureless compound by adopting the structure of an
    # ELEMENT-NEUTRAL same-name twin: an accession sharing its alias whose recount is
    # C/N/S/P = (0,0,0,0), so it can neither absorb nor emit a mapped atom. The
    # question here is whether the same trick would give the thermo members a
    # structure, and the answer is a ceiling that needs no twins machinery to take.
    #
    # `element_counts.parquet` already sits in the bake's seams with an exact recount
    # for every compound and is read by NO direction-lane code; this is what it is
    # for. The name key is looser than `twins`' alias keys, so this OVER-counts what
    # the real predicate could reach -- which is the right direction for a ceiling.
    ec = pd.read_parquet(BAKE / "seams" / "element_counts.parquet",
                         columns=["mnxm", "element", "n_atoms"])
    piv = ec.pivot_table(index="mnxm", columns="element", values="n_atoms",
                         aggfunc="first")
    neutral_ids = set(piv.index[piv.notna().all(axis=1) & (piv == 0).all(axis=1)])
    structured = {m for m in neutral_ids if (props.get(m) or {}).get("smiles")}

    by_name = collections.defaultdict(set)
    for m, n in names.items():
        if n:
            by_name[n.strip().lower()].add(m)
    neutral_twin = {}
    for mnxr in still:
        for m in unreadable(stoich[mnxr][0]):
            if m not in neutral_twin:
                n = (names.get(m) or "").strip().lower()
                neutral_twin[m] = bool((by_name.get(n, set()) - {m}) & structured)
    neutral_rescue = {mnxr for mnxr in still
                      if unreadable(stoich[mnxr][0])
                      and all(neutral_twin.get(m) for m in unreadable(stoich[mnxr][0]))}
    row("element_neutral_twin", neutral_rescue, "DEAD END",
        f"every structureless blocker has an element-neutral same-name twin. Only "
        f"{len(neutral_ids):,} of {len(piv):,} compounds are element-neutral at all "
        f"and {len(structured):,} of those carry a structure -- the predicate has "
        f"almost nothing to work with, and this is a ceiling on a looser key than "
        f"`aam_blockers` uses")

    df = pd.DataFrame(rows, columns=["mechanism", "reactions", "in_graph",
                                     "confidence", "note"])
    print()
    print(df[["mechanism", "reactions", "in_graph", "confidence"]].to_string(index=False))
    print(f"\nresidual after every mechanism above: "
          f"{len(still - carrier_only - wildcard_only - unb - neutral_rescue):,} "
          f"tier-0 reactions no named mechanism reaches")

    # ---- who is doing the blocking, and how concentrated is it -----------
    # The number that decides whether a carrier table is a week or a year. It is
    # taken per MECHANISM because the two underspecifications are largely different
    # accessions of the same kind of compound -- a table built from one list will
    # miss most of the other, which is not visible in a combined ranking.
    print("\nblockers among the still-silent tier-0 population")
    blocked = post[(post["member"] == "dgbyg") & post["mnxr"].isin(still)]
    for mech in ("no_smiles", "wildcard"):
        s = blocked.loc[blocked["mechanism"] == mech, "blocker"].value_counts()
        if not len(s):
            continue
        car = [m for m in s.index if CARRIER.search(names.get(m, "") or "")]
        cum = (s[car].cumsum() / s[car].sum()) if car else None
        top = {k: f"{cum.iloc[k-1]:.0%}" for k in (20, 100) if cum is not None and len(car) >= k}
        print(f"  {mech:<10} {int(s.sum()):>7,} reactions · {len(s):>6,} distinct blockers"
              f" · {len(car):>5,} generic carriers covering {int(s[car].sum()):,}"
              f" ({s[car].sum()/s.sum():.0%}); carrier concentration {top}")
        for m, n in s.head(6).items():
            print(f"      {n:>5,}  {m:<14} {(names.get(m) or '?')[:52]}")
    if a.out:
        df.to_csv(a.out, sep="\t", index=False)
        print(f"wrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
