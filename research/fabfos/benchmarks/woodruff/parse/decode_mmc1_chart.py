#!/usr/bin/env python3
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

C = "http://schemas.openxmlformats.org/drawingml/2006/chart"

PPTX_REL = "data/fabfos/originals/benchmarks/woodruff/1-s2.0-S1096717613000098-mmc1.pptx"
CHART_PART = "ppt/charts/chart1.xml"

REF_PROD = "'New Summary'!$C$2:$C$4115"
REF_WT15 = "'New Summary'!$D$2:$D$4115"
REF_WT30 = "'New Summary'!$E$2:$E$4115"

ANCHORS = {1052: ("betI", 4.4755), 1993: ("betB", 2.7420), 2243: ("betA", 2.4028)}
ANCHOR_TOL = 1e-3

DATASET = "scales_prod"
OUT_REL = "data/fabfos/benchmarks/_extractions/scales_prod/extraction.tsv"

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

PHENO_NAMED = "not_enriched"
PHENO_OTHER = "unlabelled"


def repo_root() -> Path:
    for d in Path(__file__).resolve().parents:
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit("could not locate repo root (no ancestor contains data/fabfos)")


def read_num_caches(pptx: Path, part: str) -> dict[str, dict[int, float]]:
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
    return [i + 1 for i in range(1, len(values)) if values[i] > values[i - 1]]


def write_extraction(
    path: Path,
    slots: list[int],
    prod: dict[int, float],
    wt15: dict[int, float],
    wt30: dict[int, float],
) -> None:
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
