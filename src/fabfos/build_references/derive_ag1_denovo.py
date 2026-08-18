"""AG1's de-novo GPR, from DH1's, by withholding the genes its genotype broke.

    PATH="/home/tony/lib/miniforge3/envs/msm-fabfos/bin:$PATH" \\
        python build_references/derive_ag1_denovo.py [--publish]

B2 emits one table per proteome, and AG1 has no proteome: it is absent from NCBI, which
is the whole reason it borrows DH1. The GEM side handles that inside `host_gpr_gem.py`,
where a borrow is a map plus an edit list; the de-novo side cannot, because that step
runs on a compute node over mapper outputs and knows nothing about strains. So the borrow
happens here, once, against the table B2 produced.

ONLY THE LOSS ALLELES ARE WITHHELD. A genotype lists markers, not knockouts:
`check_ag1_identity.py`'s MARKERS carries what each one does, and `gyrA96` is a
resistance allele whose gyrase still works. Withholding it would claim AG1 has no DNA
gyrase -- not viable, and invisible to anything downstream. Four of the seven are losses.

A WITHHELD ROW, NOT A DROPPED REACTION, and the difference is the whole reason to do it
this way. The GEM edit list names a reaction (`GTPDPK`) because a curated model's unit is
a reaction. A de-novo table's unit is an ORF, and AG1's `relA1` is a broken PROTEIN --
so what AG1 lacks is relA's rows, and whatever else still nominates a reaction keeps
nominating it. Dropping by MNXR instead would remove `GDPDPK` from spoT as well, which is
a claim about a gene AG1 has intact.

The marker list is imported from `check_ag1_identity.py` rather than restated: that file
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
    """Walk up until a directory holding `data/fabfos` is found."""
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_ag1_identity import MARKERS                                 # noqa: E402

sys.path.insert(0, str(REPO / "src" / "metasmith_libraries" / "resources" / "lib"))
import fabfos_evidence as fe                                           # noqa: E402

GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"
DH1 = "e_coli_dh1"
AG1 = "e_coli_ag1"


def proteins_for(host: str, symbols: set[str]) -> dict[str, list[str]]:
    """symbol -> the ORF ids that host's proteome gives it.

    Keyed on the record id the lanes key on -- the first token of the header -- because
    that is what the mapper's `orf` column carries. A symbol with no record is reported
    by the caller rather than skipped: a marker that names nothing is a fact, and a
    marker that names nothing *because the lookup was wrong* looks identical until
    someone checks.
    """
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
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="src", type=Path,
                    default=REPO / "data/fabfos/runs" / DH1 / "gpr" / "gpr_denovo.parquet")
    ap.add_argument("--publish", action="store_true",
                    help=f"write data/fabfos/{AG1}/gpr/gpr_denovo.parquet")
    a = ap.parse_args()

    if not a.src.exists():
        raise SystemExit(
            f"no DH1 de-novo table at {a.src.relative_to(REPO)}.\n  Build it: "
            f"`python research/fabfos/examples/clone_gpr_on_hpc.py --orfs "
            f"data/fabfos/originals/genomes/{DH1}/genome/NC_017638.1.faa --into "
            f"data/fabfos/runs/{DH1} --site sockeye --run`, then --publish")
    d = pd.read_parquet(a.src)
    # THE PARENT IS CHECKED BEFORE ANYTHING IS BORROWED FROM IT. A borrow inherits
    # whatever the parent got wrong, so a short-lane parent would have produced a
    # short-lane derivative with no sign that anything was missing.
    fe.validate_gpr(d, "chosen_4", None, str(d["source"].iat[0]), fe.extensions_of(d))
    symbols = {sym for sym, kind in MARKERS.values() if sym and kind == "loss"}
    found = proteins_for(DH1, symbols)
    print(f"{a.src.name}: {len(d):,} rows, {d['orf'].nunique():,} ORFs")
    for sym in sorted(symbols):
        print(f"    {sym:<5} -> {found[sym] or 'no record in the proteome'}")

    withheld = {o for ids in found.values() for o in ids}
    keep = d[~d["orf"].isin(withheld)].copy()
    lost = d[d["orf"].isin(withheld)]
    keep["host"] = AG1
    keep["build_id"] = keep["build_id"].astype(str) + f"_{AG1}"

    out = REPO / "data" / "fabfos" / "runs" / AG1 / "gpr"
    if not a.publish:
        out = Path(__file__).resolve().parent / "out" / AG1
    out.mkdir(parents=True, exist_ok=True)
    keep.to_parquet(out / "gpr_denovo.parquet", index=False, compression="zstd")
    (out / "BUILD_borrow.json").write_text(json.dumps(dict(
        borrowed_from=DH1, source=str(a.src.relative_to(REPO)),
        genotype={k: dict(gene=v[0], allele=v[1]) for k, v in MARKERS.items()},
        withheld_kind="loss",
        withheld_orfs=sorted(withheld),
        rows_dh1=len(d), rows_ag1=len(keep), rows_withheld=len(lost),
        mnxr_only_in_dh1=sorted(set(lost["mnxr"]) - set(keep["mnxr"])),
    ), indent=2))

    only = sorted(set(lost["mnxr"]) - set(keep["mnxr"]))
    print(f"\nwithheld {len(lost)} row(s) over {len(withheld)} ORF(s); AG1 keeps "
          f"{len(keep):,} of {len(d):,}")
    print(f"    reactions no other ORF still nominates: {only}")
    print(f"\n-> {out}/gpr_denovo.parquet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
