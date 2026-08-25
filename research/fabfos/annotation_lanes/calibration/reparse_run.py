# THE QUESTION -- can a run's GPR table be rebuilt under the new weighting without
# re-running a single annotator?
#
# SCOPE   One host at a time, from the six lane files DVC already holds beside it. The
#         embedding pass is a CPU matmul against cached vectors, so no GPU is needed
#         and nothing here touches a queue.
# INPUT   data/fabfos/runs/<host>/annotations/lanes/*   (six files, DVC-tracked)
#         data/fabfos/originals/genomes/<host>/genome/*.faa
#         data/fabfos/processed/{mnxr_lookup,label_transfer_landmarks,lane_calibration}
# METHOD  `gpr_4lane.py` for the mapper table, then `host_denovo_from_mapper.py` for the
#         attribution/feature/universe blocks. Both are the shipped code, called as the
#         transform calls them -- this driver exists to supply the paths, not to
#         reimplement either.
# ENV     mamba run -n msm python research/fabfos/annotation_lanes/calibration/reparse_run.py \
#             --host e_coli_k12 --out <dir>
# OUT     <dir>/<host>/{gpr_denovo_mapper.parquet,gpr_denovo.parquet,BUILD_denovo.json}
#
# **CAUTION** This writes OUTSIDE the run directory by default. Publishing a rebuilt
# table over a DVC-pinned one is a separate decision from being able to rebuild it.
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _cal as C  # noqa: E402

LIB = C.REPO / "src/metasmith_libraries/resources/lib"
PROC = C.REPO / "data/fabfos/processed"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--threads", type=int, default=8)
    # A cohort run has no entry under originals/genomes -- its ORFs are clone inserts,
    # not a proteome -- so it names its FASTA and stops after the mapper. The blocks a
    # cohort table carries are the collector's to add, and the collector is not this.
    ap.add_argument("--orfs", type=Path)
    ap.add_argument("--mapper-only", action="store_true")
    # For a table whose extension blocks a collector added and this driver cannot
    # rebuild -- a cohort table's condition/action columns, say. The mapper decides
    # which calls exist and what each is worth, so grafting its `llr` onto the old
    # table by grain key gives the same result as re-collecting, PROVIDED the row sets
    # are identical. That is asserted rather than assumed: if the two disagree the
    # graft is refused, because a differing row set means the mapper changed what the
    # table contains and only a real re-collect can say what the blocks should hold.
    ap.add_argument("--graft-onto", type=Path)
    a = ap.parse_args()

    run = C.REPO / "data/fabfos/runs" / a.host
    lanes = run / "annotations/lanes"
    if not lanes.is_dir():
        raise SystemExit(
            f"no {lanes}. Seven of the thirteen runs have their lane files "
            f"materialised locally; `dvc pull {run.relative_to(C.REPO)}/annotations.dvc` "
            f"fetches the rest")
    if a.orfs:
        faa = [a.orfs]
    else:
        faa = sorted((C.REPO / "data/fabfos/originals/genomes" / a.host
                      / "genome").glob("*.faa"))
        if len(faa) != 1:
            raise SystemExit(f"expected one proteome under {a.host}/genome, found {faa}")

    dest = a.out / a.host
    dest.mkdir(parents=True, exist_ok=True)
    mapper = dest / "gpr_denovo_mapper.parquet"

    # `--source` is the mapper's stamp on every row and becomes `genome_unit` downstream, so
    # it names the ORF set rather than the host: two hosts differing by one reaction
    # would otherwise be indistinguishable in a concatenated table.
    subprocess.run([sys.executable, str(LIB / "fabfos_gpr/gpr_4lane.py"),
                    "--ev-lib", str(LIB / "fabfos_evidence.py"),
                    "--orfs", str(faa[0]),
                    "--kofam", str(lanes / "kofamscan.csv"),
                    "--clean", str(lanes / "clean.tsv"),
                    "--uniref", str(lanes / "diamond_uniref50.tsv"),
                    "--uniref-descriptions", str(lanes / "diamond_uniref50_descriptions.tsv"),
                    "--pbert-emb", str(lanes / "proteinbert_embeddings.parquet"),
                    "--bridge", str(PROC / "mnxr_lookup/mnxr_lookup.parquet"),
                    "--landmarks", str(PROC / "label_transfer_landmarks/landmarks"),
                    "--calibration", str(PROC / "lane_calibration"),
                    "--lane-set", "chosen_4", "--source", faa[0].stem,
                    "--threads", str(a.threads), "--out", str(mapper)], check=True)

    if a.graft_onto:
        import pandas as pd
        new = pd.read_parquet(mapper)
        old = pd.read_parquet(a.graft_onto)
        key = ["source", "orf", "channel", "intermediate_id", "mnxr"]
        kn = set(map(tuple, new[key].astype(str).to_numpy()))
        ko = set(map(tuple, old[key].astype(str).to_numpy()))
        if kn != ko:
            raise SystemExit(
                f"[graft] the reparsed mapper and {a.graft_onto.name} do not carry the "
                f"same rows: {len(kn - ko):,} new, {len(ko - kn):,} gone. The graft is "
                f"only sound while the mapper changes what a call is WORTH and not "
                f"which calls exist -- re-collect this table instead")
        merged = old.drop(columns=["raw_score", "score_kind"], errors="ignore").merge(
            new[key + ["llr"]], on=key, how="left", validate="one_to_one")
        cols = [c for c in C.fe.schema_for(C.fe.extensions_of(merged))]
        merged = merged[cols]
        dest_g = dest / a.graft_onto.name
        merged.to_parquet(dest_g, index=False, compression="zstd")
        print(f"\n[graft] {len(merged):,} rows, llr from the reparse -> {dest_g}")
        return 0

    if a.mapper_only:
        print(f"\n[reparse] {a.host} mapper -> {mapper}")
        return 0

    subprocess.run([sys.executable,
                    str(C.REPO / "src/fabfos/build_references/host_denovo_from_mapper.py"),
                    "--host", a.host, "--mapper", str(mapper)], check=True)
    built = (C.REPO / "src/fabfos/build_references/out" / a.host / "gpr_denovo.parquet")
    if built.exists():
        (dest / "gpr_denovo.parquet").write_bytes(built.read_bytes())
        j = built.with_name("BUILD_denovo.json")
        if j.exists():
            (dest / "BUILD_denovo.json").write_bytes(j.read_bytes())
    print(f"\n[reparse] {a.host} -> {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
