"""Turn model output into curated crosswalk rows, filtered to what `admit()` will accept.

Two harvests come out of one prompt and this is the per-metabolite one: every stand-in
SMILES the model assigned to a structureless accession, as candidate rows for the curated
crosswalk `curation.py` merges. (The per-reaction harvest — the simplified balanced
equation — is the run JSONL itself, and feeds a real AAM pass.)

**`admit()` aborts rather than skips, so filtering happens here.** That is the whole
reason this file exists. A curated crosswalk is a small hand-authored artifact and a bad
row in it is an authoring error, so `admit` treats the first one as fatal — correct for a
human-written file, fatal for a machine-written one. One hallucinated row would otherwise
kill the bake. Every gate `admit` applies is therefore applied here first, and a row that
would fail is dropped with its reason counted:

    1. additive only    the accession must not already have a structure
    2. stale id         `mnx_name` must equal the metabolite table's name exactly
    3. self-check       `count_struct(smiles, element)` must equal the asserted `n_atoms`
    4. citation         `basis` must be non-empty

Gate 3 cannot fail here because this computes `n_atoms` from the SMILES rather than
believing an assertion about it — which is the same reason the balance is recounted rather
than read. Gate 4 is filled from the model's own `why`, extended with the reactions that
vouched for the row.

**Only balanced rewrites vouch.** A substitution proposed inside an equation that does not
balance has no evidence behind it beyond the model's say-so; inside one that does, the
recount is a real constraint the stand-in had to satisfy. Substitutions from refused,
unbalanced and unusable rewrites are dropped.

**Report both coverages and never let one stand for the other.** Per-metabolite coverage
runs well ahead of per-reaction coverage because fixing two of a reaction's three blockers
still leaves it unmapped, and the blocker tail is flat — 11,050 distinct blockers over the
12,417-reaction residual, the top 100 covering only 20%.

    PYTHONPATH=src mamba run -n rdkit-scratch python \\
        research/fabfos/llm_curation/harvest.py \\
        --run runs/aam_r1.dev.jsonl --panel panel/dev.jsonl \\
        --lookups data/fabfos/processed/lookups --out crosswalk_llm.tsv
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(HERE))

from arbiter import Resolver, judge, load_jsonl                              # noqa: E402
from ecspr.bake.aam.curation import CROSSWALK_COLS, count_struct             # noqa: E402
from ecspr.bake.direction.refdata import load_mnxm_props                     # noqa: E402
from measure_rescue import MNX                                               # noqa: E402

# The four elements the crosswalk carries, matching `Refs.counts_of` and the existing
# curated rows. A stand-in contributes a row per element it actually contains.
ELEMENTS = ("C", "N", "S", "P")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", type=Path, required=True, nargs="+",
                    help="one or more runner JSONL outputs")
    ap.add_argument("--panel", type=Path, required=True, nargs="+")
    ap.add_argument("--lookups", type=Path,
                    default=ROOT / "data/fabfos/processed/lookups")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--min-vouches", type=int, default=1,
                    help="how many balanced rewrites must propose a row to keep it")
    a = ap.parse_args()

    mets = pd.read_parquet(a.lookups / "metabolites.parquet",
                           columns=["mnxm", "name", "smiles", "has_smiles"])
    name_of = dict(zip(mets["mnxm"], mets["name"]))
    has_struct = set(mets.loc[mets["has_smiles"], "mnxm"])
    res = Resolver(load_mnxm_props(MNX / "chem_prop.tsv"))

    panel = {}
    for p in a.panel:
        panel.update({r["mnxr"]: r for r in load_jsonl(p)})
    runs = [r for p in a.run for r in load_jsonl(p)]

    # A substitution is only as good as the equation it was proposed inside.
    proposals = collections.defaultdict(list)   # (mnxm, smiles) -> [(mnxr, why)]
    verdicts = collections.Counter()
    reactions_balanced, blockers_seen, blockers_fixed = set(), set(), set()

    for rec in runs:
        m = rec.get("mnxr")
        if m in panel:
            blockers_seen.update(panel[m].get("blockers", []))
        v = judge(rec, res)["verdict"]
        verdicts[v] += 1
        if v != "balanced":
            continue
        reactions_balanced.add(m)
        for s in rec.get("substitutions") or []:
            mnxm, smi = str(s.get("id", "")), str(s.get("smiles", ""))
            if mnxm and smi:
                proposals[(mnxm, smi)].append((m, str(s.get("why", "")).strip()))

    rows, dropped = [], collections.Counter()
    # One metabolite may be proposed several structures by different reactions. Keep the
    # most-vouched and record the disagreement rather than silently picking one: "one
    # metabolite, one lane" is enforced downstream by LANE_PRIORITY, but a lane that
    # contradicts itself internally is this file's problem to resolve.
    by_met = collections.defaultdict(list)
    for (mnxm, smi), vouches in proposals.items():
        by_met[mnxm].append((len(vouches), smi, vouches))
    contested = 0

    for mnxm, cands in sorted(by_met.items()):
        if len(cands) > 1:
            contested += 1
        cands.sort(key=lambda c: (-c[0], c[1]))
        n_vouch, smi, vouches = cands[0]
        if n_vouch < a.min_vouches:
            dropped["too few vouches"] += 1
            continue
        if mnxm in has_struct:                                      # gate 1
            dropped["already has a structure"] += 1
            continue
        if mnxm not in name_of:                                     # gate 2, missing id
            dropped["not in the metabolite table"] += 1
            continue
        why = next((w for _, w in vouches if w), "")
        if not why:                                                 # gate 4
            dropped["no citation from the model"] += 1
            continue
        counts = {X: count_struct(smi, X) for X in ELEMENTS}
        if any(c is None for c in counts.values()):                 # gate 3, unparseable
            dropped["SMILES does not parse"] += 1
            continue
        if not any(counts.values()):
            dropped["stand-in carries no tracked atoms"] += 1
            continue
        cited = ", ".join(sorted(m for m, _ in vouches)[:4])
        basis = (f"LLM curation lane: {why} Proposed in {n_vouch} rewrite(s) that "
                 f"balanced on a per-element recount ({cited}).")
        for X, n in counts.items():
            if n:
                rows.append({"mnxm": mnxm, "smiles": smi, "mnx_name": name_of[mnxm],
                             "element": X, "n_atoms": n, "basis": basis, "lane": "llm"})
        blockers_fixed.add(mnxm)

    df = pd.DataFrame(rows, columns=list(CROSSWALK_COLS))
    a.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.out, sep="\t", index=False)

    print(f"=== HARVEST from {len(runs)} responses over {len(panel)} panel reactions")
    for k, n in verdicts.most_common():
        print(f"  {k:<12} {n}")
    print(f"\n  substitutions proposed inside balanced rewrites : {len(proposals)}")
    print(f"  distinct metabolites proposed                   : {len(by_met)}")
    print(f"  of those, proposed more than one structure      : {contested}")
    for k, n in dropped.most_common():
        print(f"    dropped, {k:<34} {n}")
    print(f"\n  ADMITTED: {len(blockers_fixed)} metabolites, {len(df)} element rows")

    # The two numbers that must never substitute for each other.
    print(f"\n  per-metabolite coverage : {len(blockers_fixed)}/{len(blockers_seen)} "
          f"blockers in this panel = "
          f"{len(blockers_fixed) / max(len(blockers_seen), 1):.1%}")
    fully = sum(1 for m, p in panel.items()
                if p.get("blockers") and set(p["blockers"]) <= blockers_fixed)
    print(f"  per-reaction coverage   : {fully}/{len(panel)} reactions have EVERY "
          f"blocker covered = {fully / max(len(panel), 1):.1%}")
    print(f"  (reactions the model itself balanced: {len(reactions_balanced)})")
    print(f"\n  -> {a.out}")


if __name__ == "__main__":
    main()
