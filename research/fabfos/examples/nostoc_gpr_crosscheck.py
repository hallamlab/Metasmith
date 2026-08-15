#!/usr/bin/env python3
"""Compare the canonical GPR tables against the 2026-07-09 ad-hoc annotation.

    python research/fabfos/examples/nostoc_gpr_crosscheck.py --prior <dir>

A FREE INDEPENDENT CHECK, NOT GROUND TRUTH. The earlier run annotated these exact
protein files by a different method -- two deep-learning annotators wired together
by hand, KO through a DIAMOND-to-KEGG lane, and no `clean` or `kofam` lane at all.
So a channel-by-channel equality is not expected and would in fact be suspicious.

What IS expected, and what this checks:

  * the ORF identifier sets must be IDENTICAL per organism. Both runs read the same
    FASTA, so any difference is one of them parsing or filtering headers, not biology.
  * the reaction counts should be the same ORDER OF MAGNITUDE. A canonical lane
    reaching ten times fewer reactions than the ad-hoc equivalent is a truncated
    reference, not a stricter method.
  * whatever the two share should overlap substantially on MNXR. Two independent
    annotations of one proteome that agree on almost nothing are not both right.

A large discrepancy is a finding. Nothing here exits non-zero on one, because
"different method, different numbers" is the expected state and a gate that cried
wolf would be turned off.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
NOSTOC = REPO / "data" / "fabfos" / "nostoc"
ORGANISMS = ["NOS", "ERY", "RHI"]

# THE PRIOR IS NOT IN THIS REPOSITORY and was never versioned. It is the ad-hoc
# 2026-07-09 pass, which lived in the old fabfos project's shared unversioned
# `data/` directory -- a workspace-level path that survives that repository being
# archived, because it was never repository content in the first place. It is not
# migrated here on purpose: nothing downstream consumes it, and the comparison it
# supports is already recorded in `data/fabfos/nostoc/annotation/PROVENANCE.md`.
# Pass `--prior` if it has moved; this is a re-runnable check, not a gate.
DEFAULT_PRIOR = Path("/home/tony/agentic_workspace/data/fabfos/community_ecspr/annotation")
PRIOR_TABLES = ["reaction_evidence_ko.parquet", "reaction_evidence_dl.parquet"]


def main() -> int:
    import pandas as pd

    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prior", default=str(DEFAULT_PRIOR))
    a = ap.parse_args()

    prior_dir = Path(a.prior)
    frames = [pd.read_parquet(prior_dir / f) for f in PRIOR_TABLES
              if (prior_dir / f).exists()]
    if not frames:
        print(f"no prior tables under {prior_dir}", file=sys.stderr)
        return 1
    prior = pd.concat(frames, ignore_index=True)

    print("prior (2026-07-09, ad hoc) channels: "
          f"{sorted(prior['channel'].unique())}\n")

    for org in ORGANISMS:
        table = NOSTOC / "annotation" / org / "gpr_4lane.parquet"
        if not table.exists():
            print(f"{org}: canonical table not published yet ({table.relative_to(REPO)})")
            continue
        new = pd.read_parquet(table)
        old = prior[prior["organism"] == org]

        faa = NOSTOC / "orfs" / f"{org}.faa"
        n_faa = sum(1 for line in faa.open() if line.startswith(">"))

        new_orfs, old_orfs = set(new["orf"]), set(old["orf"])
        only_new, only_old = new_orfs - old_orfs, old_orfs - new_orfs
        new_mnxr, old_mnxr = set(new["mnxr"]), set(old["mnxr"])
        shared = new_mnxr & old_mnxr

        print(f"=== {org} ({n_faa:,} proteins) ===")
        print(f"  canonical: {len(new):,} rows, {len(new_orfs):,} ORFs, "
              f"{len(new_mnxr):,} MNXR   channels {sorted(new['channel'].unique())}")
        print(f"  ad hoc:    {len(old):,} rows, {len(old_orfs):,} ORFs, "
              f"{len(old_mnxr):,} MNXR")
        # The ORF id sets are the part that must agree: both read one FASTA.
        print(f"  ORF ids:   {len(new_orfs & old_orfs):,} shared, "
              f"{len(only_new):,} canonical-only, {len(only_old):,} ad-hoc-only"
              + ("  <-- both annotations should key on the same headers"
                 if (only_new or only_old) else "  (identical)"))
        if only_new or only_old:
            for label, s in (("canonical-only", only_new), ("ad-hoc-only", only_old)):
                if s:
                    print(f"      {label} e.g. {sorted(s)[:3]}")
        print(f"  MNXR:      {len(shared):,} shared "
              f"({100.0 * len(shared) / max(len(new_mnxr), 1):.0f}% of canonical, "
              f"{100.0 * len(shared) / max(len(old_mnxr), 1):.0f}% of ad hoc)")
        ratio = len(new_mnxr) / max(len(old_mnxr), 1)
        if not 0.2 <= ratio <= 5.0:
            print(f"  ORDER-OF-MAGNITUDE GAP: canonical reaches {ratio:.2f}x the ad-hoc "
                  f"reaction count. Suspect a truncated reference before biology.")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
