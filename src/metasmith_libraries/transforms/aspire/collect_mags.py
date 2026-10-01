# collect_mags -- no .nf process. Lays one dedup run's 95% centroid bins out as the ASV linker's
# `--genome-qc-dir`, which upstream took from a separate genome QC pipeline.
#
# The linker matches the FASTA under genomes_subset/ and the GFF under barrnap/ by file stem, so
# both are named for the bin. The QC table keys its rows as `genome_id`, not `Bin Id`: a
# `Bin Id` row makes the linker look the FASTA up through a path column, and a path written here
# does not exist where the linker runs, so every genome is dropped.
#
# The QC table carries no taxonomy: GTDB-Tk takes `sequences::putative_genome`, which a quality
# bin is not, so nothing in this library can classify one.

import csv
import shutil
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm   = model.AddRequirement(lib.GetType("sequences::assembly"))
table = model.AddRequirement(lib.GetType("binning_local::cluster_table"), parents={asm})
bins  = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"), parents={asm})
gffs  = model.AddRequirement(lib.GetType("binning_local::quality_bin_rrna_gff"), parents={bins})
out   = model.AddProduct(lib.GetType("aspire::mag_collection"))

CENTROID_COL = "is_centroid_95"


def _centroids(path: Path) -> list[str]:
    with open(path) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    assert rows and CENTROID_COL in rows[0], f"[{path.name}] has no [{CENTROID_COL}] column"
    return sorted(r["bin_id"] for r in rows if r[CENTROID_COL].strip() == "1")


def _stats(fasta: Path) -> tuple[int, int, int]:
    lengths, n = [], 0
    with open(fasta) as f:
        for line in f:
            if line.startswith(">"):
                lengths.append(0)
            else:
                lengths[-1] += len(line.strip())
    total = sum(lengths)
    for length in sorted(lengths, reverse=True):
        n += length
        if 2 * n >= total:
            return len(lengths), total, length
    return len(lengths), total, 0


def _n16s(gff: Path) -> int:
    with open(gff) as f:
        return sum(1 for line in f if not line.startswith("#") and "16S" in line)


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    by_stem = {p.local.stem: p for p in context.InputGroup(bins)}
    gff_of = {}
    for g in context.InputGroup(gffs):
        b = context.SourceOf(g, bins)
        assert b is not None, f"[{g.local.name}] has no bin among this task's inputs"
        gff_of[b.local.stem] = g

    picked = _centroids(context.Input(table).local)
    missing = [b for b in picked if b not in by_stem or b not in gff_of]
    assert not missing, f"{len(missing)} centroid bins lack a FASTA or a GFF here: {missing[:5]}"

    genomes, barrnap = iout.local / "genomes_subset", iout.local / "barrnap"
    genomes.mkdir(parents=True)
    barrnap.mkdir()
    rows = []
    for b in picked:
        shutil.copy(by_stem[b].local, genomes / f"{b}.fna")
        shutil.copy(gff_of[b].local, barrnap / f"{b}.gff")
        num_seqs, sum_len, n50 = _stats(by_stem[b].local)
        rows.append([b, num_seqs, sum_len, n50, _n16s(gff_of[b].local)])
    with open(iout.local / "Master_genome_QC.tsv", "w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["genome_id", "num_seqs", "sum_len", "N50", "contains_16S"])
        w.writerows(rows)
    Log.Info(f"{len(rows)} centroid bins, {sum(r[-1] > 0 for r in rows)} with a 16S gene")

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / "Master_genome_QC.tsv").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    # One collection per dedup run, as derep_mag_reference makes one reference per run.
    group_by=table,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
