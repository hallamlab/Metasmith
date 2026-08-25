"""LW06's de-novo GPR, from BW25113's, by withholding the genes its genotype broke.

    PATH="/home/tony/lib/miniforge3/envs/msm-fabfos/bin:$PATH" \\
        python src/fabfos/build_references/derive_lw06_denovo.py [--publish]

LW06 has no submitted genome sequence, which is why the GEM side registers it as a borrow
in `host_gpr_gem.py` and why the de-novo side has to borrow here: B2 emits one table per
proteome, and that step runs on a compute node over mapper outputs and knows nothing about
strains. This is `derive_ag1_denovo.py` for a different pair, and deliberately the same
shape -- AG1 borrows DH1, LW06 borrows BW25113.

BW25113 ITSELF WITHHOLDS NOTHING, and that is worth stating rather than leaving as an
absent function. GCF_050858555.1 is BW25113's own sequence, so its proteome already lacks
what the strain lacks: `lacZ`, `araB` and `rhaB` return no record, exactly as the Keio
parent's genotype says they should. The only BW25113 marker still present as a protein is
`recA`, which the tolerance study deletes -- and recA nominates no metabolic reaction, so
withholding it would change nothing and asserting it here would imply it might.

A WITHHELD ROW, NOT A DROPPED REACTION. The GEM edit list names reactions because a curated
model's unit is a reaction; a de-novo table's unit is an ORF. LW06 lacks the *proteins*
ldhA, ackA, frdABCD and adhE, so what it lacks is those ORFs' rows, and whatever else still
nominates a reaction keeps nominating it. Dropping by MNXR instead would strip reactions
that intact genes still support -- which is the same distinction that makes `ACKr` survive
on purT/tdcD in the curated channel.

THE ENGINEERED INSERTION IS NOT ADDED HERE. pdcZm and adhBZm are Zymomonas genes on a Tn7
insertion and appear in no E. coli proteome; they ride as study GPR rows
(`data/fabfos/runs/woodruff_clones/gpr/gpr_insertion.parquet`) that concatenate at solve time. This
file only subtracts, like every other host step.

The genotype is imported from `check_lw06_identity.py` rather than restated: that file
measures which markers the MODEL can see, this one applies the same markers to the lanes,
and two copies of a genotype is how they come to disagree.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_lw06_identity import BW25113_MARKERS, LW06_MARKERS          # noqa: E402

sys.path.insert(0, str(REPO / "src" / "metasmith_libraries" / "resources" / "lib"))
import fabfos_evidence as fe                                           # noqa: E402

GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"
PARENT = "e_coli_bw25113"
LW06 = "e_coli_lw06"

EXPECT_ABSENT_IN_PARENT = {"lacZ", "araB", "rhaB"}


def proteins_for(host: str, symbols: set[str]) -> dict[str, list[str]]:
    faa = sorted((GENOMES / host / "genome").glob("*.faa"))
    if len(faa) != 1:
        raise SystemExit(f"expected one proteome under {host}/genome, found {faa}")
    out: dict[str, list[str]] = {s: [] for s in symbols}
    for line in faa[0].open():
        if not line.startswith(">"):
            continue
        m = re.search(r"\[gene=([^\]]+)\]", line)
        if m and m.group(1).strip() in out:
            out[m.group(1).strip()].append(line[1:].split()[0])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--from", dest="src", type=Path,
                    default=REPO / "data/fabfos/runs" / PARENT / "gpr" / "gpr_denovo.parquet")
    ap.add_argument("--publish", action="store_true",
                    help=f"write data/fabfos/runs/{LW06}/gpr/gpr_denovo.parquet")
    a = ap.parse_args()

    if not a.src.exists():
        raise SystemExit(
            f"no BW25113 de-novo table at {a.src.relative_to(REPO)}.\n  Build it: "
            f"`python research/fabfos/examples/clone_gpr_on_hpc.py --orfs "
            f"data/fabfos/originals/genomes/{PARENT}/genome/CP193896.1.faa --into "
            f"data/fabfos/runs/{PARENT} --site sockeye --run`, then --publish, then "
            f"rename gpr_denovo_mapper.parquet to gpr_denovo.parquet")
    d = pd.read_parquet(a.src)
    fe.validate_gpr(d, "chosen_4", None, str(d["source"].iat[0]), fe.extensions_of(d))
    print(f"{a.src.name}: {len(d):,} rows, {d['orf'].nunique():,} ORFs")

    parent_losses = {sym for sym, kind in BW25113_MARKERS.values()
                     if sym and kind == "loss"}
    present = proteins_for(PARENT, parent_losses)
    unexpected = sorted(s for s in EXPECT_ABSENT_IN_PARENT if present.get(s))
    if unexpected:
        raise SystemExit(
            f"{PARENT}'s proteome still carries {unexpected}, which its genotype deletes. "
            f"That is not BW25113's sequence -- check the genome registration before "
            f"deriving anything from it.")
    print(f"  parent check: {sorted(EXPECT_ABSENT_IN_PARENT)} absent from its own "
          f"proteome, as the Keio parent genotype requires")
    still_there = {s: v for s, v in present.items() if v}
    print(f"  parent markers still present as proteins (not withheld, see docstring): "
          f"{sorted(still_there)}")

    symbols = {sym for sym, kind in LW06_MARKERS.values() if sym and kind == "loss"}
    found = proteins_for(PARENT, symbols)
    for sym in sorted(symbols):
        print(f"    {sym:<5} -> {found[sym] or 'NO RECORD in the proteome'}")
    missing = sorted(s for s, v in found.items() if not v)
    if missing:
        raise SystemExit(
            f"LW06 deletes {missing}, but {PARENT}'s proteome has no record under that "
            f"symbol. A deletion that names nothing withholds nothing, so this would "
            f"silently produce LW06 == BW25113.")

    withheld = {o for ids in found.values() for o in ids}
    keep = d[~d["orf"].isin(withheld)].copy()
    lost = d[d["orf"].isin(withheld)]
    keep["host"] = LW06
    keep["build_id"] = keep["build_id"].astype(str) + f"_{LW06}"

    out = REPO / "data" / "fabfos" / "runs" / LW06 / "gpr"
    if not a.publish:
        out = Path(__file__).resolve().parent / "out" / LW06
    out.mkdir(parents=True, exist_ok=True)
    keep.to_parquet(out / "gpr_denovo.parquet", index=False, compression="zstd")
    only = sorted(set(lost["mnxr"]) - set(keep["mnxr"]))
    (out / "BUILD_borrow.json").write_text(json.dumps(dict(
        borrowed_from=PARENT, source=str(a.src.relative_to(REPO)),
        genotype={k: dict(gene=v[0], allele=v[1])
                  for k, v in {**BW25113_MARKERS, **LW06_MARKERS}.items()},
        withheld_kind="loss",
        withheld_symbols=sorted(symbols), withheld_orfs=sorted(withheld),
        parent_withholds_nothing=("GCF_050858555.1 is BW25113's own sequence; its "
                                  "proteome already lacks lacZ/araB/rhaB"),
        rows_parent=len(d), rows_lw06=len(keep), rows_withheld=len(lost),
        mnxr_only_in_parent=only,
    ), indent=2))

    print(f"\nwithheld {len(lost):,} row(s) over {len(withheld)} ORF(s); LW06 keeps "
          f"{len(keep):,} of {len(d):,}")
    print(f"    reactions no other ORF still nominates: {len(only)} {only[:10]}")
    print(f"\n-> {out}/gpr_denovo.parquet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
