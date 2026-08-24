# What else is in MetaCyc's reactions.dat, and is any of it worth a vote?
#
# `metacyc_flatfile.load_reactions` returns four slots -- unique_id, direction, left, right --
# from a record carrying thirty-four attributes. This script scores every one that could
# plausibly carry directional evidence, and the answer for all of them is no. THE READER
# STAYS AT FOUR SLOTS BECAUSE OF WHAT IS BELOW, not because nobody looked: a field carried
# into a schema that nothing reads is a maintenance obligation bought with nothing. It exists
# so that "no" is a measurement rather than an opinion, and so the numbers in the r10 record
# can be re-derived from the pinned MetaCyc 26 and MNXref 4.5 chunks.
#
# THE ORIENTATION TRAP IS THE WHOLE REASON THIS IS NON-TRIVIAL. MNXref re-canonicalises
# equation orientation on import, and 61% of the GIBBS-0-bearing records need a sign flip to
# be stated in MNXR orientation. A naive metacyc->MNXR join inverts them silently, which would
# make a bad number look like a good one on exactly half the corpus. Every arm below routes
# through `curated.flip_verdict`, the same alignment the direction label already gets.
#
# WHAT IT MEASURES, AND WHAT IT FOUND
#
#   GIBBS-0 -- a group-contribution dGr'0 for 14,766 reactions, 14,051 distinct MNXR after
#   alignment, never read by anything. Rejected twice over. As a MEMBER: on the tier-2 rows
#   where eQuilibrator and GIBBS-0 both speak and both are under 200 kJ/mol, eQuilibrator gets
#   79.4% of curated signs right and GIBBS-0 65.4%; on the rows where they disagree in sign,
#   eQuilibrator is right 71.7% and GIBBS-0 25.8%; averaging them is worse than eQuilibrator
#   alone. Its robust residual against eQuilibrator is 19.0 kJ/mol, so its own sigma would sit
#   at or above DIR_SIGMA_0 = 23.489 -- a member carrying no information past the prior. And it
#   buys no coverage: 205 of the 36,151 tier-0 reactions. As a VALIDITY SIGNAL, which is the
#   obvious fallback for a number too noisy to vote: the unbalanced fraction across bands of
#   |eq - GIBBS-0| rises and then FALLS, so it predicts nothing monotonically, and the ensemble
#   gets *more* accurate as the gap widens. A large disagreement with GIBBS-0 is evidence the
#   ensemble is right.
#
#   The same test built on |eq - dgbyg| -- the two CORRELATED members the lane already has --
#   is monotone and reaches further. That is the negative result's useful half: the suspect
#   signal r10 wants was already sitting in the annotation as two columns nothing compares.
#
#   pathways.dat REACTION-LAYOUT -- an explicit :DIRECTION per reaction per pathway, 12,409
#   reactions, 12,253 unanimous across their own pathways. REJECTED, and the number that
#   rejects it is coverage: of the 11,494 unanimous calls whose reaction MetaCyc also
#   describes, ZERO belong to a reaction with no reaction-level REACTION-DIRECTION. The
#   pathway layout can only restate an arrow or contradict one, and it restates: over the
#   10,774 directional arrows carrying a unanimous pathway call it agrees on 10,768, 99.94%.
#   Its whole non-duplicated content is 720 reactions the arrow calls REVERSIBLE, and
#   overriding a reaction-level arrow is the one thing a flux convention must not do -- 156
#   reactions carry conflicting directions across their own pathways, which is glycolysis
#   against gluconeogenesis showing up in the data. No held-out label can score that override
#   either: the label there IS reversible.
#
#   enzrxns.dat REACTION-DIRECTION -- 8,260 enzyme-level calls over 5,989 reactions, every one
#   of which already carries a reaction-level arrow. Zero coverage, and an enzyme-level call
#   cannot be aligned to an MNXR independently of the reaction it belongs to.
#
#   PHYSIOLOGICALLY-RELEVANT? -- T on 19,572 of 19,597 records. Discriminates nothing.
#
#   REACTION-BALANCE-STATUS -- :BALANCED on 18,361 records, :UNBALANCED-UNFIXABLE on 851,
#   :UNDETERMINED on 385. Real signal and already held: 485 of the 503 crosswalked
#   :UNBALANCED-UNFIXABLE records land on an MNXR MetaNetX ALSO calls unbalanced, which the
#   lane's own `dG_suspect` already flags. It adds 18 rows out of 83,795. Rejected as
#   subsumed, not as uninformative.
#
#   EC-NUMBER -- 15,508 records. Its one plausible use is explaining the MNXRs whose several
#   contributing MetaCyc reactions disagree about direction. There are 38 such MNXR in the
#   whole crosswalk, and different ECs explain 17 of them.
#
# GIBBS-0 IS ONLY EVER SET ON BALANCED REACTIONS, so the "wild values are unbalanced equations"
# hypothesis is not available: MetaCyc filters at source, and 37.6% of what survives is still
# past 100 kJ/mol.
#
# Run: mamba run -n rdkit-scratch python research/fabfos/bake/poc_metacyc_fields.py
from __future__ import annotations

import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.bake.direction.curated import flip_verdict           # noqa: E402
from ecspr.bake.direction.metacyc_flatfile import iter_records  # noqa: E402
from ecspr.bake.direction import refdata                        # noqa: E402

MC = ROOT / "data/fabfos/originals/metacyc/26/data"
MNX = ROOT / "data/fabfos/originals/metanetx/4.5"
ANNOT = ROOT / "data/fabfos/processed/metabolism_bake/seams/direction_annotation.parquet"
BLAST = ROOT / "research/fabfos/bake/work/blast_radius.parquet"

KCAL_TO_KJ = 4.184
# Past this the number is not a claim about a reaction any thermodynamics recognises, and
# leaving it in lets a gross tail agreement stand in for a correlation (r 0.890 -> 0.589).
MODERATE_KJ = 200.0


def implied_sign(category) -> int:
    # A curated call as the sign of dG'(left->right): RIGHT-TO-LEFT means positive.
    c = str(category)
    if "RIGHT-TO-LEFT" in c:
        return 1
    if "LEFT-TO-RIGHT" in c:
        return -1
    return 0


def load_reaction_fields() -> list[dict]:
    out = []
    for rec in iter_records(MC / "reactions.dat"):
        ids = rec.get("UNIQUE-ID")
        if not ids:
            continue
        g = rec.get("GIBBS-0")
        out.append(dict(
            unique_id=ids[0],
            direction=(rec.get("REACTION-DIRECTION") or [None])[0],
            gibbs=(float(g[0]) if g else None),
            balance=(rec.get("REACTION-BALANCE-STATUS") or [None])[0],
            ec=tuple(rec.get("EC-NUMBER") or ()),
            phys_relevant=(rec.get("PHYSIOLOGICALLY-RELEVANT?") or [None])[0],
            left=list(rec.get("LEFT") or []),
            right=list(rec.get("RIGHT") or []),
        ))
    return out


def align_gibbs(records, id2mnxr, sides, cmap) -> pd.DataFrame:
    # One row per GIBBS-0-bearing record, with dG restated in MNXR orientation.
    rows = []
    for r in records:
        if r["gibbs"] is None:
            continue
        mnxr = id2mnxr.get(r["unique_id"])
        if mnxr is None:
            rows.append(dict(mnxr=None, reason="no_mnxr", dg=None, **r))
            continue
        flipped, agree, flip = flip_verdict(r["left"], r["right"], sides.get(mnxr), cmap)
        if flipped is None:
            why = ("no_mnxr_sides" if sides.get(mnxr) is None
                   else ("no_overlap" if agree + flip == 0 else "tie"))
            rows.append(dict(mnxr=mnxr, reason=why, dg=None, **r))
            continue
        dg = r["gibbs"] * KCAL_TO_KJ * (-1.0 if flipped else 1.0)
        rows.append(dict(mnxr=mnxr, reason=("flipped" if flipped else "same"), dg=dg, **r))
    return pd.DataFrame(rows)


def pathway_votes() -> dict:
    # metacyc reaction -> Counter of the :DIRECTION its pathways lay it out in.
    layout = re.compile(r"\((\S+)\s.*?:DIRECTION\s+:(L2R|R2L)")
    votes: dict[str, Counter] = defaultdict(Counter)
    for pwy in iter_records(MC / "pathways.dat"):
        for entry in pwy.get("REACTION-LAYOUT", []):
            m = layout.match(entry)
            if m:
                votes[m.group(1)][m.group(2)] += 1
    return votes


def pathway_vs_arrow(records, votes) -> pd.DataFrame:
    # The reaction-level arrow against the unanimous pathway call, in METACYC's OWN
    # orientation. No MNXR alignment is needed and none is wanted: both are stated against
    # the same LEFT/RIGHT slots, and aligning applies the same flip to both.
    by_id = {r["unique_id"]: r for r in records}
    tab: Counter = Counter()
    for mc_id, v in votes.items():
        rec = by_id.get(mc_id)
        if rec is None or len(v) != 1 or not rec["direction"]:
            continue
        tab[(rec["direction"], next(iter(v)))] += 1
    rows = sorted({k[0] for k in tab})
    return pd.DataFrame({d: {c: tab[(d, c)] for c in ("L2R", "R2L")} for d in rows}).T


def pathway_directions(records, id2mnxr, sides, cmap, votes):
    # metacyc reaction -> aligned L2R/R2L, unanimous calls only.
    by_id = {r["unique_id"]: r for r in records}
    conflicting = sum(1 for v in votes.values() if len(v) > 1)
    rows = []
    for mc_id, v in votes.items():
        if len(v) != 1 or mc_id not in by_id:
            continue
        mnxr = id2mnxr.get(mc_id)
        if mnxr is None:
            continue
        rec = by_id[mc_id]
        flipped, _, _ = flip_verdict(rec["left"], rec["right"], sides.get(mnxr), cmap)
        if flipped is None:
            continue
        d = next(iter(v))
        if flipped:
            d = "R2L" if d == "L2R" else "L2R"
        rows.append(dict(mnxr=mnxr, pathway_direction=d, mc_arrow=rec["direction"]))
    return pd.DataFrame(rows).drop_duplicates("mnxr"), len(votes), conflicting


def banded(frame, col, bands, describe):
    for lo, hi in bands:
        s = frame[(frame[col] >= lo) & (frame[col] < hi)]
        if len(s) < 20:
            continue
        print(f"  {lo:5g}-{hi:<8g} n={len(s):6d}   {describe(s)}")


def main():
    records = load_reaction_fields()
    print(f"MetaCyc 26 reactions.dat: {len(records)} records")
    print(f"  with GIBBS-0                  {sum(1 for r in records if r['gibbs'] is not None)}")
    print(f"  with REACTION-DIRECTION       {sum(1 for r in records if r['direction'])}")
    print("  PHYSIOLOGICALLY-RELEVANT?     "
          f"{Counter(r['phys_relevant'] for r in records).most_common()}")
    print(f"  GIBBS-0 balance status        "
          f"{Counter(r['balance'] for r in records if r['gibbs'] is not None).most_common()}")

    id2mnxr = refdata.load_source_to_mnxr(MNX / "reac_xref.tsv", "metacyc.reaction")
    sides = refdata.load_mnxr_sides(MNX / "reac_prop.tsv")
    cmap = refdata.load_metacyc_compound_to_mnxm(MNX / "chem_xref.tsv")

    raw = align_gibbs(records, id2mnxr, sides, cmap)
    print("\n=== GIBBS-0 orientation alignment ===")
    print(raw.reason.value_counts().to_string())
    aligned = raw[raw.reason.isin(["same", "flipped"])].dropna(subset=["dg", "mnxr"])
    n_flip = int((aligned.reason == "flipped").sum())
    print(f"  aligned {len(aligned)} rows -> {aligned.mnxr.nunique()} MNXR; "
          f"{n_flip} ({n_flip / len(aligned):.1%}) needed a sign flip")

    gibbs = (aligned.groupby("mnxr")
             .agg(dg_mc=("dg", "median"), n_mc=("dg", "size")).reset_index())

    annot = pd.read_parquet(ANNOT)
    frame = annot.merge(gibbs, on="mnxr", how="left")
    if BLAST.exists():
        frame = frame.merge(pd.read_parquet(BLAST)[["mnxr", "in_graph"]], on="mnxr", how="left")
    frame["curated_sign"] = frame.biocyc_category.map(implied_sign)

    print("\n=== coverage: dir_tier x has GIBBS-0 ===")
    print(pd.crosstab(frame.dir_tier, frame.dg_mc.notna(), margins=True).to_string())

    both = frame[(frame.dir_tier == 2) & (frame.curated_sign != 0)
                 & frame.eq_dg.notna() & frame.dg_mc.notna()]
    both = both[(both.eq_dg.abs() < MODERATE_KJ) & (both.dg_mc.abs() < MODERATE_KJ)]
    eq, mc, truth = both.eq_dg.astype(float), both.dg_mc.astype(float), both.curated_sign
    print(f"\n=== GIBBS-0 as a MEMBER: head-to-head on {len(both)} tier-2 rows "
          f"(both speak, both under {MODERATE_KJ:g} kJ/mol) ===")
    print(f"  eQuilibrator sign correct   {np.mean(np.sign(eq) == truth):.1%}")
    print(f"  GIBBS-0 sign correct        {np.mean(np.sign(mc) == truth):.1%}")
    print(f"  the two averaged            {np.mean(np.sign((eq + mc) / 2) == truth):.1%}")
    clash = np.sign(eq) != np.sign(mc)
    print(f"  they disagree on {int(clash.sum())} rows: "
          f"eQuilibrator right {np.mean(np.sign(eq[clash]) == truth[clash]):.1%}, "
          f"GIBBS-0 right {np.mean(np.sign(mc[clash]) == truth[clash]):.1%}")
    resid = mc - eq
    print(f"  r = {np.corrcoef(eq, mc)[0, 1]:.3f}, robust residual "
          f"{1.4826 * np.median(np.abs(resid - np.median(resid))):.1f} kJ/mol "
          f"(DIR_SIGMA_0 = 23.489)")

    balanced = {k: v[1] for k, v in refdata.load_mnxr_stoich(MNX / "reac_prop.tsv").items()}
    frame["balanced"] = frame.mnxr.map(balanced)
    bands = [(0, 5), (5, 15), (15, 40), (40, 100), (100, 1e9)]

    print("\n=== GIBBS-0 as a VALIDITY SIGNAL: does |eq - GIBBS-0| predict a broken equation? ===")
    g = frame[frame.dg_mc.notna() & frame.eq_dg.notna()].copy()
    g["gap"] = (g.eq_dg.astype(float) - g.dg_mc.astype(float)).abs()
    banded(g, "gap", bands, lambda s: f"unbalanced {1 - s.balanced.mean():6.1%}")
    print("  ... it rises then falls. Compare |eq - dgbyg|, the pair the lane already has:")
    k = frame[frame.eq_dg.notna() & frame.dgbyg_dg.notna()].copy()
    k["gap"] = (k.eq_dg.astype(float) - k.dgbyg_dg.astype(float)).abs()
    banded(k, "gap", bands, lambda s: f"unbalanced {1 - s.balanced.mean():6.1%}")

    print("\n  and does the gap predict a WRONG ensemble call? (tier-2, curated-labelled)")
    d = g[(g.curated_sign != 0) & (g.dir_tier == 2)]
    banded(d, "gap", bands,
           lambda s: f"dG_raw sign correct {np.mean(np.sign(s.dG_raw) == s.curated_sign):6.1%}")
    print("  ... accuracy RISES with the gap. Disagreeing with GIBBS-0 is a good sign.")

    votes = pathway_votes()
    pwy, n_seen, n_conflict = pathway_directions(records, id2mnxr, sides, cmap, votes)
    print(f"\n=== pathways.dat REACTION-LAYOUT ===")
    print(f"  reactions with a pathway direction {n_seen}, conflicting across pathways {n_conflict}")
    # THE COVERAGE NUMBER IS THE ONE THAT DECIDES IT. A source that only ever restates an
    # arrow already read cannot move a held-out score, whatever else is true of it.
    known = {r["unique_id"] for r in records}
    arrow = {r["unique_id"] for r in records if r["direction"]}
    unanimous = {k for k, v in votes.items() if len(v) == 1}
    print(f"  unanimous and a known reaction {len(unanimous & known)}, of which with NO "
          f"reaction-level arrow {len(unanimous & known - arrow)}")
    cross = pathway_vs_arrow(records, votes)
    print("  reaction-level arrow x unanimous pathway call:")
    print("    " + cross.to_string().replace("\n", "\n    "))
    directional = cross.drop(index="REVERSIBLE", errors="ignore")
    agree = (directional.loc[[i for i in directional.index if "LEFT-TO-RIGHT" in i], "L2R"].sum()
             + directional.loc[[i for i in directional.index if i.endswith("RIGHT-TO-LEFT")],
                               "R2L"].sum())
    print(f"  it agrees with the arrow on {agree} of {int(directional.to_numpy().sum())} "
          f"({agree / directional.to_numpy().sum():.2%}) -- a restatement, not a second "
          f"opinion")
    p = frame.merge(pwy, on="mnxr", how="inner")
    in_graph = int(p.in_graph.fillna(False).sum()) if "in_graph" in p else -1
    print(f"  aligned to {len(p)} MNXR, in-graph {in_graph}")
    print(f"  of which the curated member calls REVERSIBLE: "
          f"{int((p.biocyc_category == 'REVERSIBLE').sum())}")

    print("\n=== REACTION-BALANCE-STATUS ===")
    bal = pd.DataFrame([dict(mc_id=r["unique_id"], balance=r["balance"]) for r in records])
    bal["mnxr"] = bal.mc_id.map(id2mnxr)
    bal = bal.dropna(subset=["mnxr"])
    bal["mnx_balanced"] = bal.mnxr.map(balanced)
    bal = bal.dropna(subset=["mnx_balanced"])
    print(pd.crosstab(bal.balance, bal.mnx_balanced).to_string())
    print("  ... MetaNetX already calls almost all of them unbalanced, and the lane's own "
          "dG_suspect reads that. The field's whole addition is the disagreeing corner.")
    sc = bal.merge(frame[["mnxr", "dir_tier", "dG_raw", "curated_sign"]], on="mnxr")
    sc = sc[(sc.curated_sign != 0) & (sc.dir_tier == 2) & sc.dG_raw.notna()]
    for b, g in sc.groupby("balance"):
        print(f"    {b:<24} n={len(g):5d}  ensemble sign correct "
              f"{np.mean(np.sign(g.dG_raw) == g.curated_sign):6.1%}")

    print("\n=== EC-NUMBER on the direction conflicts it might explain ===")
    per = pd.DataFrame([dict(mc_id=r["unique_id"], direction=r["direction"], ec=r["ec"])
                        for r in records if r["direction"]])
    per["mnxr"] = per.mc_id.map(id2mnxr)
    tally = Counter()
    for _mnxr, g in per.dropna(subset=["mnxr"]).groupby("mnxr"):
        if g.direction.nunique() <= 1:
            continue
        ecs = [e for e in g.ec if e]
        tally["conflicts"] += 1
        tally["fewer than two ECs" if len(ecs) < 2
              else ("one shared EC" if len(set(ecs)) == 1 else "different ECs")] += 1
    print("  " + ", ".join(f"{k}={v}" for k, v in tally.items()))

    enz = list(iter_records(MC / "enzrxns.dat"))
    with_dir = [r for r in enz if "REACTION-DIRECTION" in r and "REACTION" in r]
    reactions = {r["REACTION"][0] for r in with_dir}
    arrow = {r["unique_id"] for r in records if r["direction"]}
    print(f"\n=== enzrxns.dat ===")
    print(f"  enzyme-level directions {len(with_dir)} over {len(reactions)} reactions")
    print(f"  reactions with an enzyme-level call but NO reaction-level arrow: "
          f"{len(reactions - arrow)}")


if __name__ == "__main__":
    main()
