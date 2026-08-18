#!/usr/bin/env python3
"""Recover the genome-wide fitness distribution of the SCALEs *production* paper
from the numeric cache embedded in its supplementary PowerPoint.

Source
------
`data/fabfos/originals/benchmarks/scales/1-s2.0-S1096717613000098-mmc1.pptx`
    Supplementary file 1 of Woodruff, Boyle & Gill, "Engineering improved ethanol
    production in Escherichia coli with a genome-wide approach", Metab. Eng. 17
    (2013) 1-11.  Host strain LW06 (BW25113 dldhA dackA dfrdABCD dadhE
    attTn7::PLlacO-1 pdcZm adhBZm; ATCC BAA-2466), an engineered ethanologen.

What is read
------------
Chart part `ppt/charts/chart1.xml` — the Fig. S1 scatter of production fitness
against tolerance fitness.  It holds two `<c:ser>` elements that between them
reference three columns of an external workbook, each with a full `<c:numCache>`
of 4114 slots, 4103 of them populated:

    'New Summary'!$C$2:$C$4115   y of both series; axis title
                                 "Production gene fitness batch 8" — THIS work's
                                 final (8th batch, 33 generations) production
                                 selection.  This is the ranking column.
    'New Summary'!$D$2:$D$4115   x of series "15 g/L Tolerance"
    'New Summary'!$E$2:$E$4115   x of series "30 g/L Tolerance"

Column C appears twice (once per series); the two caches are asserted identical.

Why row position is the rank
----------------------------
The workbook rows were sorted on column C before the chart was made: C is
monotone non-increasing across all 4103 populated rows.  That is asserted here
rather than assumed — `check_monotone()` counts every position where a value
exceeds its predecessor and the script refuses to continue if the count is
non-zero.  Given the sort, the 1-based position of a row in column C *is* the
published "rank of final gene fitness".

Why the genes are anonymous
---------------------------
The chart carries only the numeric cache.  Row labels lived in the source
workbook, which survives in the .pptx solely as a dead OLE link to the author's
Dropbox (`<c:externalData>`), so the gene symbol for a given row is
unrecoverable.  The only rows that can be named are the handful the paper's
prose quotes a rank for.  No name is inferred by any other route; in particular
column D/E values are NOT joined against the companion wild-type-host paper's
supplementary table — Fig. S1's caption states the prior selections were
recalculated for this work, and the ranges confirm it (D maxes at 853.41 here
vs 140.13 there), so that join measures a different quantity.

The anchor check
----------------
Section 3.4 of the paper reports, for the betaine-biosynthesis operon that was
*not* enriched, "rank of final gene fitness: betA=2243; betB=1993; betI=1052".
The decode is only credible if those three positions in the recovered column C
carry the values the published figure was drawn from, so the script exits
non-zero unless rank 1052 -> 4.4755, 1993 -> 2.7420 and 2243 -> 2.4028 (the
paper's own rounding; compared at 1e-3).  A wrong part, a wrong column, an
off-by-one in the idx mapping or a re-sort all break this.

Output
------
Diagnostics on stdout, and — unless `--no-write` is passed — the extraction table
`data/fabfos/benchmarks/_extractions/scales_prod/extraction.tsv`, one row per
rank.  Anonymous rows get the synthetic id `scales_prod_rNNNN`.

Stdlib only, by design: the extraction must reproduce from a bare checkout.
"""

import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

C = "http://schemas.openxmlformats.org/drawingml/2006/chart"

PPTX_REL = "data/fabfos/originals/benchmarks/scales/1-s2.0-S1096717613000098-mmc1.pptx"
CHART_PART = "ppt/charts/chart1.xml"

REF_PROD = "'New Summary'!$C$2:$C$4115"
REF_WT15 = "'New Summary'!$D$2:$D$4115"
REF_WT30 = "'New Summary'!$E$2:$E$4115"

# rank -> (gene, value quoted/implied by the prose)
ANCHORS = {1052: ("betI", 4.4755), 1993: ("betB", 2.7420), 2243: ("betA", 2.4028)}
ANCHOR_TOL = 1e-3

DATASET = "scales_prod"
OUT_REL = "data/fabfos/benchmarks/_extractions/scales_prod/extraction.tsv"

# The only rows the paper's prose lets us name.  Section 3.4, one sentence.
NAMED_NOTE = (
    "although betaine supplementation improves ethanol tolerance under these "
    "production conditions, the genes responsible for biosynthesis were not "
    "enriched during the selection (rank of final gene fitness: betA=2243; "
    "betB=1993; betI=1052)"
)
NAMED = {rank: gene for rank, (gene, _) in ANCHORS.items()}

HEADER = [
    "dataset", "rank", "gene", "gene_norm",
    "fitness_prod", "fitness_wt15", "fitness_wt30", "named", "phenotype", "note",
]

# THIS ARM IS NOT BINARISABLE, AND THE PHENOTYPE COLUMN SAYS SO RATHER THAN HIDING IT.
#
# The companion study thresholds at fitness > 1 and the paper's own counts confirm it
# (158 at 15 g/L, 487 at 30 g/L). Applying the same cut HERE labels 3,475 of 4,103 genes
# enriched -- 85% of the genome -- because this is a different column on a different
# scale (median 2.64, max 4,675 against the companion's max 140). The production paper
# quotes no threshold of its own; it argues entirely in ranks. So a numeric cut here
# would be a number this benchmark invented and then scored itself against.
#
# What the paper DOES claim about named genes is one sentence, and it is negative: the
# three betaine-biosynthesis genes were NOT enriched. Those three rows get `not_enriched`
# and every other row gets `unlabelled` -- which is the honest shape of "the screen
# measured this gene and the paper makes no claim about it", and is distinct from a
# measured negative.
#
# The paper's one positive claim -- that the uspC-otsA-otsB locus was most highly
# enriched -- names genes but quotes no rank, so it cannot be attached to a row here
# without fabricating one. It is carried instead as the trehalose axis of
# `data/fabfos/originals/benchmarks/scales/gof_scales.tsv`, where it is a cited claim
# rather than a row of this table.
PHENO_NAMED = "not_enriched"
PHENO_OTHER = "unlabelled"


def repo_root() -> Path:
    """Nearest ancestor of this file that contains `data/fabfos`."""
    for d in Path(__file__).resolve().parents:
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit("could not locate repo root (no ancestor contains data/fabfos)")


def read_num_caches(pptx: Path, part: str) -> dict[str, dict[int, float]]:
    """Every `<c:numRef>` in a chart part, as formula -> {slot index: value}.

    Repeated references to the same formula must carry identical caches; that is
    checked here because column C is cached once per series.
    """
    with zipfile.ZipFile(pptx) as z:
        root = ET.fromstring(z.read(part))
    out: dict[str, dict[int, float]] = {}
    counts: dict[str, int] = {}
    for ref in root.iter(f"{{{C}}}numRef"):
        formula = ref.find(f"{{{C}}}f").text
        cache = ref.find(f"{{{C}}}numCache")
        col = {
            int(pt.get("idx")): float(pt.find(f"{{{C}}}v").text)
            for pt in cache.findall(f"{{{C}}}pt")
        }
        counts[formula] = int(cache.find(f"{{{C}}}ptCount").get("val"))
        if formula in out and out[formula] != col:
            raise SystemExit(f"inconsistent duplicate cache for {formula}")
        out[formula] = col
    for formula, col in out.items():
        print(f"  {formula}  slots={counts[formula]}  populated={len(col)}")
    return out


def check_monotone(values: list[float]) -> list[int]:
    """Positions (1-based rank) where the sort order breaks."""
    return [i + 1 for i in range(1, len(values)) if values[i] > values[i - 1]]


def write_extraction(
    path: Path,
    slots: list[int],
    prod: dict[int, float],
    wt15: dict[int, float],
    wt30: dict[int, float],
) -> None:
    """One row per rank; blank cell where a series has no value at that row."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not (path.stat().st_mode & 0o200):
        raise SystemExit(
            f"{path} exists and is read-only (DVC hardlink) — "
            f"`dvc unprotect` it before rewriting"
        )
    fmt = lambda col, slot: repr(col[slot]) if slot in col else ""
    with path.open("w") as fh:
        fh.write("\t".join(HEADER) + "\n")
        for rank, slot in enumerate(slots, start=1):
            gene = NAMED.get(rank)
            named = gene is not None
            if not named:
                gene = f"{DATASET}_r{rank:04d}"
            fh.write(
                "\t".join([
                    DATASET,
                    str(rank),
                    gene,
                    gene.lower(),
                    fmt(prod, slot),
                    fmt(wt15, slot),
                    fmt(wt30, slot),
                    "TRUE" if named else "FALSE",
                    PHENO_NAMED if named else PHENO_OTHER,
                    NAMED_NOTE if named else "",
                ]) + "\n"
            )


def main() -> int:
    write = "--no-write" not in sys.argv[1:]
    root = repo_root()
    pptx = root / PPTX_REL
    print(f"repo root : {root}")
    print(f"source    : {pptx.relative_to(root)}")
    print(f"chart part: {CHART_PART}")

    cols = read_num_caches(pptx, CHART_PART)
    missing = [r for r in (REF_PROD, REF_WT15, REF_WT30) if r not in cols]
    if missing:
        print(f"FAIL: chart part is missing expected series {missing}", file=sys.stderr)
        return 1

    prod = cols[REF_PROD]
    slots = sorted(prod)
    values = [prod[i] for i in slots]
    n = len(values)

    print(f"\nrows recovered: {n}")
    for label, ref in (("prod (C)", REF_PROD), ("wt15 (D)", REF_WT15), ("wt30 (E)", REF_WT30)):
        col = cols[ref]
        v = list(col.values())
        print(f"  {label}: n={len(v)}  min={min(v):.6g}  max={max(v):.6g}")

    violations = check_monotone(values)
    if violations:
        print(
            f"FAIL: column C is not non-increasing — {len(violations)} violation(s), "
            f"first at rank {violations[0]}",
            file=sys.stderr,
        )
        return 1
    print(f"\nmonotonicity: column C is non-increasing across all {n} rows (0 violations)")

    print("\nanchor check (paper 3.4: 'rank of final gene fitness: "
          "betA=2243; betB=1993; betI=1052')")
    ok = True
    for rank in sorted(ANCHORS):
        gene, expected = ANCHORS[rank]
        if rank > n:
            print(f"FAIL: anchor rank {rank} ({gene}) beyond {n} recovered rows", file=sys.stderr)
            ok = False
            continue
        got = values[rank - 1]
        delta = abs(got - expected)
        status = "ok" if delta <= ANCHOR_TOL else "MISMATCH"
        print(f"  rank {rank:>5} {gene:<5} expected {expected:.4f}  got {got:.10g}  "
              f"delta {delta:.2e}  {status}")
        if delta > ANCHOR_TOL:
            print(
                f"FAIL: anchor rank {rank} ({gene}) expected {expected:.4f}, "
                f"got {got:.10g} (off by {delta:.6g} > {ANCHOR_TOL})",
                file=sys.stderr,
            )
            ok = False
    if not ok:
        return 1

    print("\ntop 10 ranks:")
    for r in range(1, 11):
        print(f"  {r:>3}  {values[r - 1]:.10g}")

    if write:
        out = root / OUT_REL
        write_extraction(out, slots, prod, cols[REF_WT15], cols[REF_WT30])
        print(f"\nwrote {out.relative_to(root)}  ({n} rows, {len(NAMED)} named)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
