"""How much of the direction gap is rescuable, per mechanism, measured.

The bake leaves reactions at `dir_tier=0` when no member spoke, so the ratio
defaults to 1.0 and "no evidence" is indistinguishable downstream from "genuinely
reversible" -- 47,266 of 83,795 under r7, 37,404 under r8. This script partitions
that gap by the machinery that would have to change for each part of it to move, so
a reader can price the repairs against each other instead of against a single
aggregate.

IT READS AN ANNOTATION AND `--bake` NAMES WHICH ONE, so it re-targets itself: run
against r8 the water-fix rows report what the forecast expected to move and did not,
rather than what was available to move. It is the third verifier that can be aimed at
a chunk STAGED BESIDE the deployed one -- `check_references.py --results` and
`aam_v3_nostoc.py` are the other two -- which matters because a suffixed chunk is read
by nothing, so a green suite at the deployed path proves only that the old bake works.

EVERY ROW IS A SET OPERATION over three committed artifacts and nothing else: the
named bake's direction annotation, the two `ecspr.bake.direction.forecast` tables (one
built with `--mnxm-only` to reproduce the pre-fix loader, one without), and the
bake's atom_pairs. No member is run. That is what makes the numbers cheap enough to
re-take rather than transcribe -- the r7 lesson is that a reading left as prose is
most of the cost of redoing it.

TWO DENOMINATORS, ALWAYS. A reaction with no atom pairs carries no graph edge, so
rescuing its direction changes no conductance. `in_graph` is the number that
matters to a consumer; the bare count is the number that matters to the ensemble.

WHAT CHECKS A RE-MEASURE IS READING WHAT IT THINKS IT IS. It used to be that the
carrier and wildcard rows name MetaNetX's compound table rather than the bake and so
never move. r9 substitutes structures for exactly those compounds, which retires that
check. `--expect` replaces it with a stronger one: a committed table of the numbers
this configuration produced last time, compared row for row, so a stale annotation
surfaces as a disagreement between two independent readings rather than as a
familiar-looking number. `--substitutions none` is what the r8 baseline was taken
under and what reproduces `rescue_scope.tsv`.

Reproduce. Two resolve passes -- the `--mnxm-only` build needs a `--mnxm-only`
resolution or its eq arm is not the one r7 ran -- and they need eQuilibrator, which
`rdkit-scratch` does not carry. **`resolve` checkpoints into `--out` every 2,000
compounds, so the file existing does not mean the pass finished**; wait for the process,
and check `len(resolution) == participants_with_inchikey` before building on it. A
partial table silently under-reports `unresolved` and the forecast will not notice.
    R=data/fabfos/originals/metanetx/4.5
    W=research/fabfos/benchmarks/direction_rescue/work
    for f in "--mnxm-only resolution_mnxmonly" " resolution"; do set -- $f
      PYTHONPATH=src mamba run -n build-refs-equilibrator \\
        python -m ecspr.bake.direction.forecast resolve \\
        --reac-prop $R/reac_prop.tsv --chem-prop $R/chem_prop.tsv $1 --out $W/$2.parquet
    done
    PYTHONPATH=src mamba run -n rdkit-scratch python -m ecspr.bake.direction.forecast build \\
        --reac-prop $R/reac_prop.tsv --chem-prop $R/chem_prop.tsv --mnxm-only \\
        --resolution $W/resolution_mnxmonly.parquet \\
        --out $W/forecast_asdeployed.parquet --out-summary $W/summary_asdeployed.tsv
    PYTHONPATH=src mamba run -n rdkit-scratch python -m ecspr.bake.direction.forecast build \\
        --reac-prop $R/reac_prop.tsv --chem-prop $R/chem_prop.tsv \\
        --resolution $W/resolution.parquet \\
        --out $W/forecast_postfix.parquet --out-summary $W/summary_postfix.tsv
    PYTHONPATH=src mamba run -n rdkit-scratch python \\
        research/fabfos/benchmarks/direction_rescue/measure_rescue.py --work $W \\
        --expect research/fabfos/benchmarks/direction_rescue/rescue_scope.tsv
"""
from __future__ import annotations

import argparse
import collections
import re
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
MNX = REPO / "data" / "fabfos" / "originals" / "metanetx" / "4.5"
PROCESSED = REPO / "data" / "fabfos" / "processed"

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


def load_mapped(bake: Path):
    from ecspr.bake import encoding as refs
    V = refs.load_vocab(bake / "vocab.parquet")
    rxn = V.df[V.df["kind"] == "rxn"]
    sym = dict(zip(rxn["code"], rxn["symbol"]))
    codes = pd.read_parquet(bake / "atom_pairs.parquet", columns=["rxn"])["rxn"].unique()
    return {sym[c] for c in codes}


def bake_stamp(bake: Path) -> str:
    # `<vocab_sha256>:<src_direction_sha256>` -- which bake this reading describes.
    #
    # The shared identity block alone cannot say it: it is a fact about the node space, so
    # a direction-only re-bake inherits it byte for byte. `benchmarks/bake_identity.py`
    # carries the same pairing for the decode caches.
    import json
    import pyarrow.parquet as pq
    md = pq.read_schema(bake / "direction.parquet").metadata or {}
    return (f"{json.loads(md[b'ecspr_bake'])['vocab_sha256']}:"
            f"{json.loads(md[b'ecspr_bake_file'])['src_direction_sha256']}")


def spoke(fc, member=None):
    d = fc if member is None else fc[fc["member"] == member]
    return set(d.loc[d["mechanism"] == "expected_ok", "mnxr"])


def agrees_with_forecast(ann: pd.DataFrame, post: pd.DataFrame) -> int:
    bad = 0
    for member, col in (("dgbyg", "dgbyg_dg"), ("eq", "eq_dg")):
        actually = set(ann.loc[ann[col].notna(), "mnxr"])
        predicted = spoke(post, member)
        broke = actually - predicted
        print(f"  forecast vs annotation · {member:<5} spoke {len(actually):>6,} · "
              f"predicted {len(predicted):>6,} · predicted-silent-but-spoke "
              f"{len(broke):>4,} · predicted-to-speak-but-silent "
              f"{len(predicted - actually):>6,}")
        if broke:
            print(f"    ONE-SIDEDNESS BROKEN on {member}: "
                  f"{sorted(broke)[:8]}{' ...' if len(broke) > 8 else ''}")
            bad += 1
    return bad


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", required=True, type=Path,
                    help="directory holding forecast_asdeployed.parquet and "
                         "forecast_postfix.parquet")
    ap.add_argument("--bake", default="metabolism_bake",
                    help="chunk under data/fabfos/processed, or an absolute path. Aim it "
                         "at a chunk staged beside the deployed one to verify BEFORE the "
                         "promote -- a suffixed chunk is read by nothing else")
    ap.add_argument("--substitutions", default="none",
                    help="'none' is the configuration the r8 baseline was taken under "
                         "and the one that reproduces rescue_scope.tsv. Otherwise a "
                         "substitution table directory, which MUST be the one the "
                         "forecast in --work was built with and the one the members "
                         "were run with -- it is recorded, not applied")
    ap.add_argument("--couples", type=Path, default=None,
                    help="the carrier couples table. Adds carrier.couple_admissible: the "
                         "subpopulation blocked only by carriers that HAVE a tabulated "
                         "potential, which is what a substitution can actually reach")
    ap.add_argument("--expect", type=Path, default=None,
                    help="a previous run's TSV; compared row for row and a mismatch is a "
                         "non-zero exit")
    ap.add_argument("--out", type=Path, default=None, help="write the table as TSV")
    a = ap.parse_args(argv)

    if a.substitutions != "none" and not Path(a.substitutions).is_dir():
        raise SystemExit(f"[rescue] --substitutions {a.substitutions!r} is neither "
                         f"'none' nor a substitution table directory")
    bake = Path(a.bake) if Path(a.bake).is_absolute() else PROCESSED / a.bake

    from ecspr.bake.direction.refdata import load_mnxr_stoich, load_mnxm_props
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")

    pre = pd.read_parquet(a.work / "forecast_asdeployed.parquet")
    post = pd.read_parquet(a.work / "forecast_postfix.parquet")
    ann = pd.read_parquet(bake / "seams" / "direction_annotation.parquet",
                          columns=["mnxr", "dir_tier", "eq_dg", "dgbyg_dg"])
    mapped = load_mapped(bake)
    stamp = bake_stamp(bake)
    print(f"bake {bake.name} · substitutions {a.substitutions} · {stamp}")
    if agrees_with_forecast(ann, post) != 0:
        raise SystemExit("[rescue] the forecast and the annotation disagree about which "
                         "members spoke; one of them is not describing this bake")
    names = load_names()
    stoich = load_mnxr_stoich(MNX / "reac_prop.tsv")
    props = load_mnxm_props(MNX / "chem_prop.tsv")

    tier = {t: set(ann.loc[ann["dir_tier"] == t, "mnxr"]) for t in (0, 1, 2, 3)}
    post_silent = set(ann["mnxr"]) - spoke(post)
    still = tier[0] & post_silent
    print(f"universe {len(ann):,} · tier 0 {len(tier[0]):,} "
          f"(in-graph {len(tier[0] & mapped):,}) · tier 3 {len(tier[3]):,}")
    print(f"tier 0 both members STILL silent after the water fix: {len(still):,} "
          f"(in-graph {len(still & mapped):,})")

    rows = []

    def row(key, s, confidence, note):
        rows.append((key, len(s), len(s & mapped), confidence, note))

    # ---- 1/2: the water fix, per member and per tier it lands in ----------
    #
    # THESE ROWS CHANGED MEANING WHEN r8 LANDED, and the code did not have to change for
    # them to: they intersect the forecast's gain with the DEPLOYED annotation's tier 0,
    # so against r7 they read as "available" and against r8 as "still there" -- the
    # reactions the forecast expected to move that did not. Under r8 they are the 101
    # dGbyG called unbalanced at the `heavy_atom_tolerance 1e-9` boundary.
    gained_db = spoke(post, "dgbyg") - spoke(pre, "dgbyg")
    gained_eq = spoke(post, "eq") - spoke(pre, "eq")
    row("water_fix.tier0.dgbyg", gained_db & tier[0], "high",
        "forecast to gain dGbyG and still silent in the deployed bake")
    row("water_fix.tier0.eq", gained_eq & tier[0], "high",
        "forecast to gain eQuilibrator and still silent; an upper bound unless the "
        "forecast was given --resolution")
    row("water_fix.tier0.either", (gained_db | gained_eq) & tier[0], "high",
        "forecast to leave tier 0 and still in it; r8 delivered 9,862 of 9,963")
    row("water_fix.tier3.either", (gained_db | gained_eq) & tier[3], "high",
        "curated-only rows forecast to gain a thermo vote and still without one; "
        "r8 delivered 3,800 of 3,806")

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

    if a.couples is not None:
        named = set(pd.read_csv(a.couples, sep="\t", comment="#")["mnxm"].astype(str))
        adm, adm_closed = set(), set()
        for mnxr in still:
            s = stoich[mnxr][0]
            bad = set(unreadable(s)) | set(wildcards(s))
            if bad and bad <= named:
                adm.add(mnxr)
                if remainder_balances(s, bad):
                    adm_closed.add(mnxr)
        row("carrier.couple_admissible", adm, "medium",
            f"every blocker is one of the {len(named):,} accessions in {a.couples.name} "
            f"-- a carrier with a tabulated potential, not merely a carrier-shaped name")
        row("carrier.couple_admissible.closed", adm_closed, "medium-high",
            "and the remainder already balances, so the direction is a dE'0 lookup")

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
    ec = pd.read_parquet(bake / "seams" / "element_counts.parquet",
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
    # ---- what this reading describes, carried with it ---------------------
    # A rescue table read without its configuration is how the r7 delivery split outlived
    # the r8 repin. The header names the bake, the substitution set and the forecast
    # tables, so `--expect` is comparing two readings of the SAME thing or says so.
    header = [f"# bake\t{bake.name}",
              f"# bake_stamp\t{stamp}",
              f"# substitutions\t{a.substitutions}",
              f"# couples\t{a.couples.name if a.couples else 'none'}",
              f"# forecast_asdeployed\t{_sha(a.work / 'forecast_asdeployed.parquet')}",
              f"# forecast_postfix\t{_sha(a.work / 'forecast_postfix.parquet')}"]

    rc = 0
    if a.expect is not None:
        want = pd.read_csv(a.expect, sep="\t", comment="#")
        rc = _compare(want, df, a.expect)

    if a.out:
        with open(a.out, "w") as fh:
            fh.write("\n".join(header) + "\n")
            df.to_csv(fh, sep="\t", index=False)
        print(f"wrote {a.out}")
    return rc


def _sha(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()[:16]


def _compare(want: pd.DataFrame, got: pd.DataFrame, source: Path) -> int:
    w = want.set_index("mechanism")[["reactions", "in_graph"]]
    g = got.set_index("mechanism")[["reactions", "in_graph"]]
    bad = []
    for mech in sorted(set(w.index) | set(g.index)):
        if mech not in w.index:
            bad.append(f"  + {mech:<34} new: {tuple(g.loc[mech])}")
        elif mech not in g.index:
            bad.append(f"  - {mech:<34} gone, was {tuple(w.loc[mech])}")
        elif tuple(w.loc[mech]) != tuple(g.loc[mech]):
            bad.append(f"  ~ {mech:<34} {tuple(w.loc[mech])} -> {tuple(g.loc[mech])}")
    if not bad:
        print(f"\n[expect] {len(g)} rows agree with {source} exactly")
        return 0
    print(f"\n[expect] DISAGREES with {source}:")
    print("\n".join(bad))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
