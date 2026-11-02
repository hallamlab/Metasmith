#!/usr/bin/env python3
"""Recovery comparison: our Pratama 2026 groundwater-virome run vs the published
Zenodo archive (10.5281/zenodo.17897233, staged unzipped at --archive-root).

Owned by L1 (wave 3 of the CAMI four-run campaign). Runs on fir, where both the
published archive and our run's products live as cluster paths -- like
research/cami/verify.py, this is a plain local-filesystem script, not something
that shells out over ssh itself.

Acceptance criterion 2 is a comparison on RECOVERY, not on identity: for each of
three outputs -- the vOTU catalogue, the dereplicated MAG set, and AMG calls --
this script counts each side independently and reports both counts side by side.
It does not attempt sequence-level reconciliation (no skani/blast between our
contigs and theirs); "recovery" here means "how many of the same KIND of thing
did each side produce," which is what --with-zenodo-comparison's own two
transforms (pratama_votu_recovery.py, derep_mag_reference.py) were designed to
answer with an ANI-distance comparison and were never run for this arm (their
DEFERRED inputs mint a fresh uuid4 leaf id on every solve -- see the wave 3
report -- so this script exists as the reproducible, out-of-DAG alternative).

Two non-matches are expected and NOT chased, per the wave 3 brief:
  - AMG count: the published Groundwater_AMG.faa is raw DRAM-v/VIBRANT calls,
    but Groundwater_AMG_representatives.faa is an MMseqs2-clustered subset of
    it (the paper's headline number). Ours (amg_summary.tsv) is raw calls too.
    We report against BOTH published files so raw-vs-raw is visible alongside
    raw-vs-representative.
  - MAG count: our bin quality gate is CheckM2 completeness/contamination
    thresholds, not the paper's own quality score, so counts diverge for a
    known, structural reason -- not a bug in either pipeline.

Usage:
    PYTHONPATH="$PWD/src" mamba run -n msm python research/cami/pratama_compare.py \\
        --archive-root /scratch/phyberos/pratama2026/zenodo_17897233 \\
        --run-root /scratch/phyberos/pratama2026/metasmith/runs/<key> \\
        --extract-root /scratch/phyberos/pratama2026/zenodo_17897233_unzipped
"""
import argparse
import sys
import zipfile
from pathlib import Path


def count_fasta_headers(path: Path) -> int:
    n = 0
    with path.open("rb") as f:
        for line in f:
            if line.startswith(b">"):
                n += 1
    return n


def ensure_unzipped(zip_path: Path, dest_dir: Path) -> Path:
    """Unzip zip_path into dest_dir once (idempotent via a marker file), skipping
    __MACOSX/AppleDouble entries that would otherwise double-count members whose
    names also end in the real extension (e.g. "__MACOSX/._Groundwater_AMG.faa"
    ends in ".faa" exactly like the real file)."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    marker = dest_dir / ".extracted_ok"
    if marker.exists():
        return dest_dir
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            name = Path(info.filename)
            if info.filename.startswith("__MACOSX/") or name.name.startswith("._"):
                continue
            zf.extract(info, dest_dir)
    marker.write_text("ok\n")
    return dest_dir


def published_votu_count(archive_root: Path, extract_root: Path) -> tuple[int, str]:
    zpath = archive_root / "Groundwater-votu-5k.fasta.zip"
    out = ensure_unzipped(zpath, extract_root / "votu")
    fasta = out / "FINAL_groundwater-votu-5k.fasta"
    return count_fasta_headers(fasta), str(fasta)


def published_mag_count(archive_root: Path, extract_root: Path) -> tuple[int, str]:
    total = 0
    dirs = []
    for part in (1, 2, 3):
        zpath = archive_root / f"Filtered_dereplicated_genomes_part{part}.zip"
        out = ensure_unzipped(zpath, extract_root / f"mags_part{part}")
        sub = out / f"Filtered_dereplicated_genomes_part{part}"
        n = sum(1 for p in sub.glob("*.fasta"))
        total += n
        dirs.append(f"{sub} ({n})")
    return total, " + ".join(dirs)


def published_amg_counts(archive_root: Path, extract_root: Path) -> dict:
    out = {}
    for label, fname in (("raw", "Groundwater_AMG.faa"),
                          ("representative", "Groundwater_AMG_representatives.faa")):
        zpath = archive_root / f"{fname}.zip"
        d = ensure_unzipped(zpath, extract_root / f"amg_{label}")
        fasta = d / fname
        out[label] = (count_fasta_headers(fasta), str(fasta))
    return out


def find_one(run_root: Path, filename: str) -> Path | None:
    hits = list(run_root.rglob(filename))
    return hits[0] if hits else None


def our_votu_count(run_root: Path) -> tuple[int | None, str]:
    """votu_cluster_table (mmseqs_votu.py) writes votu_membership.tsv, mmseqs
    easy-cluster's raw cluster tsv: two columns, representative-contig then
    member-contig, no header. A vOTU is one distinct representative."""
    tsv = find_one(run_root, "votu_membership.tsv")
    if tsv is None:
        return None, "votu_membership.tsv not found under run root -- step 30 " \
                      "(mmseqs_votu) has not produced output yet"
    reps = set()
    with tsv.open() as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            reps.add(line.split("\t")[0])
    return len(reps), str(tsv)


def our_mag_count(run_root: Path) -> tuple[int | None, str]:
    """binning_local::cluster_table (skani_dedup.py): bin_id, cluster_95,
    is_centroid_95, cluster_99, is_centroid_99, mean_intra_ani_95. The
    dereplicated genome set is one row per DISTINCT cluster_95 value (the
    95% ANI species-level threshold skani_dedup computes); is_centroid_95==1
    count is reported alongside as a cross-check -- the two must agree."""
    header_needle = "bin_id\tcluster_95\tis_centroid_95"
    candidate = None
    for tsv in run_root.rglob("*.tsv"):
        try:
            with tsv.open() as f:
                first = f.readline().rstrip("\n")
        except (OSError, UnicodeDecodeError):
            continue
        if first.startswith(header_needle):
            candidate = tsv
            break
    if candidate is None:
        return None, "no cluster_table (skani_dedup, header 'bin_id\\tcluster_95...') " \
                      "found under run root -- step 38 has not produced output yet"
    clusters, centroids = set(), 0
    with candidate.open() as f:
        header = f.readline().rstrip("\n").split("\t")
        idx_cluster = header.index("cluster_95")
        idx_centroid = header.index("is_centroid_95")
        for line in f:
            row = line.rstrip("\n").split("\t")
            if len(row) <= max(idx_cluster, idx_centroid):
                continue
            clusters.add(row[idx_cluster])
            centroids += 1 if row[idx_centroid] == "1" else 0
    note = str(candidate)
    if centroids != len(clusters):
        note += f"  [WARNING: centroid count {centroids} != distinct cluster count {len(clusters)}]"
    return len(clusters), note


def our_amg_count(run_root: Path) -> tuple[int | None, str]:
    """annotation::dramv_distill (dramv.py) copies DRAM-v's own distill/ dir
    whole; amg_summary.tsv is its AMG table, one data row per raw AMG call."""
    tsv = find_one(run_root, "amg_summary.tsv")
    if tsv is None:
        return None, "amg_summary.tsv not found under run root -- step 27 " \
                      "(dramv distill) has not produced output yet"
    with tsv.open() as f:
        lines = [ln for ln in f.read().splitlines() if ln.strip()]
    if not lines:
        return 0, str(tsv)
    return len(lines) - 1, f"{tsv}  (header: {lines[0][:80]}...)"


def report_line(label, published_desc, published_n, our_desc, our_n, note=""):
    print(f"\n== {label} ==")
    print(f"  published : {published_desc}")
    print(f"            -> counted {published_n}")
    print(f"  ours      : {our_desc}")
    print(f"            -> counted {our_n if our_n is not None else 'N/A'}")
    if note:
        print(f"  note      : {note}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--archive-root", type=Path, required=True,
                    help="/scratch/phyberos/pratama2026/zenodo_17897233 (the zip files, as-is)")
    ap.add_argument("--extract-root", type=Path, required=True,
                    help="scratch directory this script may write unzipped copies into")
    ap.add_argument("--run-root", type=Path, required=True,
                    help="our run's output tree, e.g. "
                         ".../pratama2026/metasmith/runs/<key> -- searched recursively")
    args = ap.parse_args()

    if not args.archive_root.exists():
        print(f"ERROR: archive root does not exist: {args.archive_root}", file=sys.stderr)
        return 1
    if not args.run_root.exists():
        print(f"ERROR: run root does not exist: {args.run_root}", file=sys.stderr)
        return 1

    pub_votu_n, pub_votu_path = published_votu_count(args.archive_root, args.extract_root)
    our_votu_n, our_votu_path = our_votu_count(args.run_root)
    report_line(
        "vOTU catalogue",
        f"{pub_votu_path} (headers >= 5kb filter, per the archive README)", pub_votu_n,
        our_votu_path, our_votu_n,
        note="counts are catalogue SIZE (recovery), not membership overlap -- no "
             "sequence comparison is performed.",
    )

    pub_mag_n, pub_mag_desc = published_mag_count(args.archive_root, args.extract_root)
    our_mag_n, our_mag_path = our_mag_count(args.run_root)
    report_line(
        "Dereplicated MAGs",
        pub_mag_desc, pub_mag_n,
        our_mag_path, our_mag_n,
        note="EXPECTED NON-MATCH: our bin quality gate is CheckM2 completeness/"
             "contamination thresholds, not the paper's own quality score.",
    )

    pub_amg = published_amg_counts(args.archive_root, args.extract_root)
    our_amg_n, our_amg_path = our_amg_count(args.run_root)
    report_line(
        "AMG calls (raw)",
        f"{pub_amg['raw'][1]} (all identified AMGs)", pub_amg["raw"][0],
        our_amg_path, our_amg_n,
        note="raw vs raw -- the closest legitimate comparison.",
    )
    report_line(
        "AMG calls (published representative set, for context only)",
        f"{pub_amg['representative'][1]} (MMseqs2-clustered per Kieft et al. 2021)",
        pub_amg["representative"][0],
        our_amg_path, our_amg_n,
        note="EXPECTED NON-MATCH: this published number is a curated/clustered "
             "subset of their own raw calls; ours is raw. Not a fair pair, shown "
             "only because it is the number the paper actually cites.",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
