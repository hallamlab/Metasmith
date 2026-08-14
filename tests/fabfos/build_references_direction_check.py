#!/usr/bin/env python3
"""What the direction assembly produced, checked against what it claimed to produce.

    python tests/build_references_direction_check.py

READS WHAT WAS RETRIEVED, not what was run. `build_references_bake_parts_on_hpc.py
direction` lands `interm::direction_annotation` at `data/temp/_seams/` and the two
evidence directories at `data/temp/metacyc_direction/` and
`data/temp/direction_calibration/`; this opens those. A run whose retrieval was wrong
reads as an absent file here rather than as a wrong number.

FOUR CHECKS, AND THREE OF THEM ARE ABOUT THE CURATED MEMBER rather than the fusion. The
fusion is table arithmetic over members that already checked themselves; the curated
member is the one place where a plausible implementation is silently wrong:

  * COVERAGE. One row per MNXR over the whole reaction universe, no duplicates. The
    combiner is built from `base_mnxrs` so this is close to guaranteed -- which is why a
    failure here means the universe itself was computed against a different release.

  * THE FLIP RATE. MetaCyc states direction in its own equation orientation and MNXref
    re-canonicalises orientation on import, so a naive metacyc->MNXR join INVERTS the
    curated call on roughly 60% of reactions. `dir_curated` re-expresses every call by
    comparing compound sets. A flip rate near zero would mean the alignment silently
    stopped running and every curated vote is a coin flip; a rate near 100% would mean it
    is inverting what it should be keeping. This number is the port-fidelity check.

  * TIER 3 (`biocyc_only`). Reactions where MetaCyc is the ONLY member that spoke. That
    is the licensed drop-in's actual contribution, and the thing that goes to zero -- with
    no other symptom -- if the drop-in is missing or unreadable, because the ensemble
    degrades to its two CORRELATED thermodynamic members and reports high confidence in
    their agreement.

  * THE TIER LADDER. Reported, not asserted: it is a description of the evidence, and
    pinning it would turn better coverage into a test failure.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"
TEMP = DATA / "temp"

ANNOTATION = TEMP / "_seams" / "direction_annotation.parquet"
CURATED_EVIDENCE = TEMP / "metacyc_direction"

# MetaNetX's reaction universe, as `dir_drive universe` computes it: every MNXR in
# reac_prop that is not the EMPTY sentinel. Recomputed here rather than trusted, because
# "one row per MNXR" is only a claim if the denominator came from somewhere else.
def universe_size() -> int | None:
    releases = sorted(p for p in (DATA / "originals" / "metanetx").glob("*") if p.is_dir())
    if len(releases) != 1:
        return None
    prop = releases[0] / "reac_prop.tsv"
    if not prop.exists():
        return None
    n = 0
    with open(prop) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            mnxr = line.split("\t", 1)[0].strip()
            if mnxr and mnxr != "EMPTY":
                n += 1
    return n


def find_curated_per_reaction() -> Path | None:
    """The per-reaction curated table, wherever `build_evidence` filed it.

    Evidence is `<tool>/<version>/`, and the version is the MetaCyc release -- which is
    exactly the thing that must not be hardcoded here.
    """
    hits = sorted(CURATED_EVIDENCE.rglob("_curated_per_reaction.parquet"))
    return hits[0] if hits else None


def main() -> int:
    if not ANNOTATION.exists():
        print(f"no annotation at {ANNOTATION.relative_to(REPO)} -- run the `direction` "
              f"part and retrieve it first.", file=sys.stderr)
        return 1

    df = pd.read_parquet(ANNOTATION)
    problems = []

    # ---- coverage --------------------------------------------------------------
    want = universe_size()
    dups = int(df["mnxr"].duplicated().sum())
    print(f"rows                  {len(df):,}")
    print(f"distinct mnxr         {df['mnxr'].nunique():,}")
    if want is not None:
        print(f"metanetx universe     {want:,}")
        if len(df) != want:
            problems.append(
                f"the annotation has {len(df):,} rows against a {want:,}-reaction "
                f"universe. The combiner is built FROM that universe, so a mismatch "
                f"means the two were computed against different MetaNetX releases.")
    if dups:
        problems.append(f"{dups:,} duplicated mnxr -- the combiner emits one row per "
                        f"base reaction, so this cannot happen by construction.")

    # ---- the tier ladder -------------------------------------------------------
    print("\ndir_tier")
    for tier, n in df["dir_tier"].value_counts().sort_index().items():
        print(f"  {tier}  {n:>8,}  ({n / len(df):5.1%})")
    print("\ndir_method (top 10)")
    for m, n in df["dir_method"].value_counts().head(10).items():
        print(f"  {m:<24} {n:>8,}")

    carry = int((df["ratio"] != 1.0).sum())
    print(f"\ncarry a direction (ratio != 1.0)   {carry:,} ({carry / len(df):.1%})")

    # ---- tier 3: the independent member's own contribution ----------------------
    biocyc_only = int((df["dir_method"] == "biocyc_only").sum())
    with_biocyc = int(df["biocyc_category"].notna().sum())
    print(f"curated call present               {with_biocyc:,}")
    print(f"tier 3 (biocyc_only)               {biocyc_only:,}")
    if with_biocyc == 0:
        problems.append(
            "NO curated calls at all. The licensed MetaCyc drop-in is the ensemble's only "
            "INDEPENDENT member -- the two thermodynamic members are both TECRDB-fitted "
            "and their agreement is discounted as shared error. Without it nothing can "
            "break a tie, and the ensemble does not say so: it reports high confidence in "
            "two correlated votes.")

    # ---- the flip rate ---------------------------------------------------------
    per_rxn = find_curated_per_reaction()
    if per_rxn is None:
        print(f"\nno _curated_per_reaction.parquet under "
              f"{CURATED_EVIDENCE.relative_to(REPO)} -- the orientation alignment cannot "
              f"be checked. The per-MNXR table alone cannot be checked against what "
              f"MetaCyc actually said, which is why the lane keeps both.", file=sys.stderr)
        problems.append("the curated member's per-reaction evidence was not retrieved.")
    else:
        cur = pd.read_parquet(per_rxn)
        decided = cur[cur["reason"].isin(("same", "flipped"))]
        flipped = int((decided["reason"] == "flipped").sum())
        rate = flipped / len(decided) if len(decided) else 0.0
        print(f"\ncurated orientation alignment ({per_rxn.parent.name})")
        print(f"  reactions with a direction       {len(cur):,}")
        print(f"  orientation decided              {len(decided):,} "
              f"({len(decided) / max(1, len(cur)):.1%})")
        print(f"  FLIPPED relative to MetaCyc      {flipped:,} ({rate:.1%})")
        for reason, n in cur["reason"].value_counts().items():
            print(f"    {reason:<20} {n:>8,}")
        # A band, not a point. The established figure is ~60%; what this is guarding
        # against is the alignment not running (near 0) or inverting its own test
        # (near 1), and both are far outside anything a release bump could move.
        if not (0.35 <= rate <= 0.85):
            problems.append(
                f"the curated flip rate is {rate:.1%}, outside the 35-85% band. MNXref "
                f"re-canonicalises orientation on import, so a faithful alignment flips "
                f"roughly 60% of MetaCyc's calls. Near zero means the alignment is not "
                f"running and every curated vote is a coin flip; near 100% means it is "
                f"inverting what it should keep. Either way the independent member is "
                f"noise wearing the shape of a vote.")

    for p in problems:
        print(f"\nFAIL: {p}", file=sys.stderr)
    if problems:
        return 1
    print("\nthe annotation covers the universe once, the curated member is present, and "
          "its orientation alignment is doing what it claims")
    return 0


if __name__ == "__main__":
    sys.exit(main())
