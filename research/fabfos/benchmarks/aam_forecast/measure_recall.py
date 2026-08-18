#!/usr/bin/env python3
"""Does the forecast's offer rule catch the silences the last run actually recorded?

    PYTHONPATH=src mamba run -n rdkit-scratch python \
        research/fabfos/benchmarks/aam_forecast/measure_recall.py

WHY THIS NUMBER AND NOT AN ACCURACY. The forecast decides which (reaction, element) gets
an element-reduced submission built for it. Its errors are not symmetric: a submission
built for a reaction that maps fine is never claimed by anything, because the layer stack
is additive and its gates refuse rather than warn -- so a false positive costs mapper time
and a false negative costs exactly the coverage the partial lane exists to add. RECALL of
the recorded silences is therefore the number that matters, and precision is reported only
to price it.

WHAT THE GROUND TRUTH IS, AND WHAT IT IS NOT. `data/fabfos/processed/metabolism_bake/logs/`
holds the previous bake's per-reaction records. That run did not converge -- Indigo was
OOM-killed at ~20k of ~44.6k attempts and the worklist stopped at 20,000 of 83,795 -- so
roughly half the universe has NO record, and a reaction absent from every table was never
asked rather than answered successfully. Every rate below is reported against the half
that WAS asked, and the size of the other half is printed beside it.

Reads two parquets and four TSVs. No mapper, no rdkit, seconds.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / "src"))

from ecspr.bake.aam.forecast import CONTEXT_WINDOW_CHARS, read_prior   # noqa: E402

LOGS = REPO / "data" / "fabfos" / "processed" / "metabolism_bake" / "logs"
LOOKUPS = REPO / "data" / "fabfos" / "processed" / "lookups"


def main() -> int:
    rx = pd.read_parquet(LOOKUPS / "reactions.parquet", columns=["mnxr", "rxn_smiles"])
    chars = {m: (len(s) if isinstance(s, str) else None)
             for m, s in zip(rx["mnxr"], rx["rxn_smiles"])}
    prior, had = read_prior(LOGS)
    if not had:
        print(f"no prior logs at {LOGS}")
        return 1

    print("=" * 74)
    print("THE THRESHOLD: RXNMapper's silence against the length of what it was sent")
    print("=" * 74)
    st = pd.read_csv(LOGS / "rxnmapper_derived_status.tsv", sep="\t")
    st["chars"] = st["mnxr"].map(chars)
    st = st[st["chars"].notna()]
    for k, g in st.groupby("derived_status"):
        q = np.percentile(g["chars"], [10, 50, 90])
        print(f"  {k:<16} n={len(g):>7,}   p10={q[0]:>6.0f}  median={q[1]:>6.0f}  "
              f"p90={q[2]:>6.0f}")
    silent = set(st.loc[st["derived_status"] != "ok", "mnxr"])
    print(f"\n  {'threshold':>9}  {'recall':>8}  {'offered':>8}  {'precision':>9}")
    for t in (256, 384, 448, CONTEXT_WINDOW_CHARS, 600, 700, 800, 1000):
        off = set(st.loc[st["chars"] > t, "mnxr"])
        mark = "  <- CONTEXT_WINDOW_CHARS" if t == CONTEXT_WINDOW_CHARS else ""
        print(f"  {t:>9}  {len(off & silent)/len(silent):>7.1%}  "
              f"{len(off)/len(st):>7.1%}  {len(off & silent)/max(1,len(off)):>8.1%}{mark}")

    print("\n" + "=" * 74)
    print("THE OFFER RULE against every silence the run recorded")
    print("=" * 74)
    # The offer rule as the module applies it, restricted to what is measurable from a
    # string: the two size caps are not predictions at all -- a reaction the adjudication
    # refuses is offered with certainty -- so only the context window is on trial here.
    offered = {m for m, c in chars.items() if c is not None and c > CONTEXT_WINDOW_CHARS}
    empirical = prior["prior_timeout"] | prior["prior_hang"] | prior["prior_empty"]
    both = offered | empirical

    ind = pd.read_csv(LOGS / "indigo_status.tsv", sep="\t")
    truth = {
        "RXNMapper returned nothing": silent,
        "Indigo timed out": set(ind.loc[ind["status"] == "timeout", "mnxr"]),
        "Indigo errored": set(ind.loc[ind["status"] == "error", "mnxr"]),
        "Indigo hung (attempted, never returned)": prior["prior_hang"],
    }
    # THE `records` COLUMN IS 100% BY CONSTRUCTION and is printed anyway, because
    # leaving it out would let the `both` column read as a result. The empirical half IS
    # these sets; what it actually CONTRIBUTES is the count printed at the bottom -- the
    # reactions it offers that the string rule does not -- and its real value is for the
    # 22.6% of the universe the prior run never reached, where a future run's records
    # will speak and no threshold on length can.
    print(f"  {'recorded silence':<42} {'n':>6}  {'string':>7} {'recorded':>8} {'both':>7}")
    for name, s in truth.items():
        if not s:
            continue
        print(f"  {name:<42} {len(s):>6,}  {len(s & offered)/len(s):>6.1%} "
              f"{len(s & empirical)/len(s):>7.1%} {len(s & both)/len(s):>6.1%}")

    print("\n" + "=" * 74)
    print("THE DENOMINATOR the prior run cannot speak for")
    print("=" * 74)
    seen = set(st["mnxr"]) | set(ind["mnxr"])
    buildable = {m for m, c in chars.items() if c is not None}
    print(f"  reactions with a buildable string       {len(buildable):>8,}")
    print(f"  ...with any record from the prior run   {len(buildable & seen):>8,}  "
          f"({len(buildable & seen)/len(buildable):.1%})")
    print(f"  ...with none                            "
          f"{len(buildable - seen):>8,}  ({len(buildable - seen)/len(buildable):.1%})")
    print(f"\n  offered by the string rule alone        {len(offered & buildable):>8,}"
          f"  ({len(offered & buildable)/len(buildable):.1%} of buildable)")
    print(f"  added by the prior run's records        "
          f"{len((empirical & buildable) - offered):>8,}")
    print(f"  offered by the two together             {len(both & buildable):>8,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
