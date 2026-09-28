#!/usr/bin/env python3
"""Scores the nf-core/mag control arm's bins with AMBER, outside any metasmith plan.

Acceptance criterion 12 scores BOTH arms by AMBER against the same gold standard, and
nf-core/mag does not run AMBER at all. Criterion 8 sanctions exactly this shape: a
post-hoc scorer whose output is identical in format to the in-plan scorer's. This script
is that scorer, and it is deliberately a transcription of two library transforms rather
than an independent implementation --
`transforms/metagenomics/binning/cami_contig_truth.py` for the gold standard and
`transforms/metagenomics/binning/amber.py` for the scoring call. It runs the same
`resources/lib/cami_gold_standard.py`, in the same polars image, and the same
`amber.py -g ... -l ... -o ... --skip_gs` in the same AMBER image, so the two arms'
`results.tsv` and `metrics_per_bin.tsv` are comparable by construction instead of by
inspection.

What makes scoring the reference arm possible at all: this campaign does not score bins
against CAMI's published `binning_gs.tsv`, whose SEQUENCEID column keys on the
CAMI-provided gold-standard assembly and shares no id space with any assembly we or
nf-core produce. It scores through the read-truth bridge, which joins reads-to-contigs
against reads-to-genomes and is therefore ASSEMBLY-AGNOSTIC. It votes over nf-core's
contigs exactly as it votes over ours.

Usage, one sample at a time:

    score_reference_amber.py \\
        --sample-id marine_sample_0 \\
        --assembly   <outdir>/Assembly/MEGAHIT/MEGAHIT-marine_sample_0.contigs.fa.gz \\
        --bam        <bam of that sample's reads against THAT assembly> \\
        --reads-mapping <cami>/.../marine_sample_0_reads/reads_mapping.tsv.gz \\
        --nfcore-contig-to-bin <outdir>/GenomeBinning/contig_to_bin/contig_to_bin_map.tsv \\
        --outdir /scratch/phyberos/reference_amber/marine_sample_0

CAUTION the assembly basename carries the assembler as a PREFIX --
`MEGAHIT-<sample>.contigs.fa.gz`, not `<sample>.contigs.fa.gz`. Verified against a real
rung-1 outdir, and getting it wrong reads as an absent file rather than a wrong one.

For the reference arm prefer `--nfcore-contig-to-bin`: nf-core/mag publishes ONE
four-column map for every binner (`mag.nf:467`, via `storeDir`, unconditionally), and
this script splits it into one AMBER label per `binner` value. `--contig-to-bin
LABEL=PATH` is for a two-column table -- our own arm's, or a hand-made one -- and it
now REFUSES a wider file rather than guessing, because the membership discriminator
only ever inspects two columns and would otherwise take `assembly_id` as the bin and
score every contig into one bin.

WARNING the BAM must be the sample's reads aligned against ITS OWN assembly, which is
what `binning_map_mode = 'own'` produces. A cross-mapped BAM names contigs from another
sample's assembly, the read-to-contig table then joins against the wrong contig set, and
the vote silently scores a different assembly. The script asserts the overlap rather
than trusting the filename.

CAUTION the column layout of nf-core/mag's own `contig_to_bin` tables was UNVERIFIED when
this was written. Rather than guess it, `_read_contig_to_bin` discriminates the contig
column by testing which column's values actually occur in the assembly, and fails loudly
if neither does. That check is worth keeping even once the layout is known: it is also
what catches a BAM or a table belonging to a different sample.
"""
import argparse
import gzip
import os
import shutil
import subprocess
import sys
from pathlib import Path

IMAGE_CACHE = Path(os.environ.get(
    "MSM_APPTAINER_CACHE", "/scratch/phyberos/cache/apptainer"))

# The same three images the transforms declare, by the names apptainer caches them
# under (slashes and colons to dots). Pinned here rather than resolved from the env
# files so this script needs no metasmith import and can run from a bare login shell.
IMAGES = {
    "samtools": "docker..staphb_samtools..1.23.sif",
    "polars":   "docker..quay.io_hallamlab_polars..1.38.1.sif",
    "amber":    "docker..quay.io_biocontainers_cami-amber..2.0.7--pyhdfd78af_0.sif",
}


def _sif(name: str) -> Path:
    p = IMAGE_CACHE / IMAGES[name]
    assert p.exists(), (
        f"image [{p}] is not in the apptainer cache. Pull it from a LOGIN node -- a "
        f"compute node moves under a megabyte per second to every external host, and a "
        f"bulk pull there HANGS rather than failing."
    )
    return p


def _run(cmd: str, image: str | None = None, binds: list[Path] | None = None) -> None:
    if image is None:
        full = cmd
    else:
        bind_args = " ".join(f"--bind {b}:{b}" for b in sorted({*(binds or [])}))
        # CAUTION `--no-home --cleanenv` and PYTHONNOUSERSITE are load-bearing, not
        # hygiene. apptainer mounts $HOME by default and CPython prepends
        # ~/.local/lib/pythonX.Y/site-packages to sys.path, so a host-installed package
        # SHADOWS the container's. Measured here: amber.py died on
        # `ImportError: libjpeg.so.62` because matplotlib imported PIL from
        # /home/phyberos/.local instead of the image's own, and the image has no
        # libjpeg. The engine's own steps escape this only because the metasmith
        # container never binds the whole home directory -- a post-hoc invocation from
        # a login shell does. Same family as apptainer inheriting fir's cert paths.
        full = (
            f"apptainer exec --no-home --cleanenv --env PYTHONNOUSERSITE=1 "
            f"{bind_args} {_sif(image)} bash -c {shlex_quote(cmd)}"
        )
    print(f"+ {full}", file=sys.stderr, flush=True)
    subprocess.run(full, shell=True, check=True)


def shlex_quote(s: str) -> str:
    import shlex
    return shlex.quote(s)


def _open_maybe_gzip(path: Path):
    return gzip.open(path, "rt") if str(path).endswith(".gz") else open(path)


def contig_lengths(assembly: Path, out: Path) -> dict[str, int]:
    """Transcribed from cami_contig_truth.py's own in-protocol length pass.

    The contig id is the header up to the first space, which is what both megahit and
    every aligner's RNAME use; keeping the description would make the BAM join miss
    every contig.
    """
    lengths: dict[str, int] = {}
    with _open_maybe_gzip(assembly) as fa:
        current, length = None, 0
        for line in fa:
            if line.startswith(">"):
                if current is not None:
                    lengths[current] = length
                current = line[1:].rstrip("\n").split(" ")[0]
                length = 0
            else:
                length += len(line.rstrip("\n"))
        if current is not None:
            lengths[current] = length
    assert lengths, f"assembly [{assembly}] yielded no contigs"
    with open(out, "w") as f:
        for contig, length in lengths.items():
            f.write(f"{contig}\t{length}\n")
    return lengths


# nf-core/mag writes ONE consolidated table for every binner rather than a file per binner:
# workflows/mag.nf:467 transposes the post-binning bin channel, splitFastas each bin for its
# headers, and collectFiles the result to
# `<outdir>/GenomeBinning/contig_to_bin/contig_to_bin_map.tsv` via `storeDir`, so it publishes
# UNCONDITIONALLY -- `refine_bins_dastool_savecontig2bin` gates a different, per-binner
# intermediate and is not what this pass needs. Its header is written as a literal in that map:
NFCORE_MAP_COLUMNS = ("assembly_id", "contig_id", "binner", "bin_id")

# Under `postbinning_input = 'both'` the same table carries all three raw binners plus DAS Tool's
# refined set, distinguished by the `binner` column: binning_refinement/main.nf:72 stamps the
# refined bins `binner: 'DASTool'`. Its line 51 REMOVES `binner` from the unbinned meta, so
# unbinned rows arrive with an empty or null binner -- they are not a bin assignment and scoring
# them as a bin literally named "null" would corrupt every metric, so they are dropped by name.
NFCORE_DROP_BINNERS = {"", "null", "none"}


def read_nfcore_contig_to_bin(path: Path, contigs: set[str]) -> dict[str, list[tuple[str, str]]]:
    """Splits nf-core's one consolidated map into one prediction set per binner."""
    with _open_maybe_gzip(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        missing = [c for c in ("contig_id", "binner", "bin_id") if c not in header]
        assert not missing, (
            f"[{path}] does not look like nf-core's contig_to_bin_map.tsv: header is {header}, "
            f"missing {missing}. Expected the columns written at workflows/mag.nf:467, "
            f"{NFCORE_MAP_COLUMNS}."
        )
        ci, bi, ni = header.index("contig_id"), header.index("binner"), header.index("bin_id")
        per_binner: dict[str, list[tuple[str, str]]] = {}
        kept = dropped_unbinned = dropped_unknown_contig = 0
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) <= max(ci, bi, ni):
                continue
            binner = parts[bi].strip()
            if binner.lower() in NFCORE_DROP_BINNERS:
                dropped_unbinned += 1
                continue
            if parts[ci] not in contigs:
                dropped_unknown_contig += 1
                continue
            per_binner.setdefault(binner, []).append((parts[ci], parts[ni]))
            kept += 1
    assert per_binner, (
        f"[{path}] yielded no scorable rows. kept={kept} "
        f"unbinned={dropped_unbinned} contig-not-in-assembly={dropped_unknown_contig}. A high "
        f"contig-not-in-assembly count means the table and the assembly are different samples."
    )
    print(
        f"  {path.name}: {kept} rows over {len(per_binner)} binners "
        f"({', '.join(sorted(per_binner))}); dropped {dropped_unbinned} unbinned, "
        f"{dropped_unknown_contig} not in this assembly",
        file=sys.stderr,
    )
    return per_binner


def _read_contig_to_bin(path: Path, contigs: set[str]) -> list[tuple[str, str]]:
    rows: list[list[str]] = []
    with _open_maybe_gzip(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("#") or line.startswith("@"):
                continue
            parts = line.split("\t") if "\t" in line else line.split(",")
            if len(parts) >= 2:
                rows.append(parts)
    # Refuse a wide table rather than guess at it. The membership test below only inspects the
    # first two columns, so on nf-core's four-column map it would find contigs in column 1 and
    # take column 0 -- `assembly_id`, constant for the whole sample -- as the bin, silently
    # scoring every contig into ONE bin. Use --nfcore-contig-to-bin for that file.
    assert max(len(r) for r in rows) <= 2 or "\t".join(rows[0][:4]).count("contig_id") == 0, (
        f"[{path}] has more than two columns. If this is nf-core's consolidated "
        f"contig_to_bin_map.tsv, pass it with --nfcore-contig-to-bin instead; a two-column "
        f"reader cannot tell its bin column from its assembly column."
    )
    assert rows, f"contig-to-bin table [{path}] has no data rows"

    # Discriminate the contig column by membership in the assembly rather than by
    # position or by header name: this is the one test that is true of the right table
    # and false of a table or BAM belonging to another sample.
    body = rows[1:] if rows[0][0] not in contigs and rows[0][1] not in contigs else rows
    hits = [sum(1 for r in body if len(r) > i and r[i] in contigs) for i in (0, 1)]
    assert max(hits) > 0, (
        f"no column of [{path}] contains any contig id from the assembly. Either the "
        f"table belongs to a different sample, or its contig ids were rewritten "
        f"(nf-core gzips and sometimes renames assembly headers). First data row: "
        f"{body[0] if body else None}"
    )
    ci = 0 if hits[0] >= hits[1] else 1
    bi = 1 - ci
    print(
        f"  {path.name}: contig column {ci}, bin column {bi}, "
        f"{hits[ci]}/{len(body)} rows match the assembly",
        file=sys.stderr,
    )
    return [(r[ci], r[bi]) for r in body if len(r) > max(ci, bi) and r[ci] in contigs]


def sample_id_of(gold: Path) -> str:
    with open(gold) as f:
        for line in f:
            if line.startswith("@SampleID:"):
                return line.strip().split(":", 1)[1]
    raise AssertionError(f"gold standard [{gold}] has no @SampleID header")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample-id", required=True)
    ap.add_argument("--assembly", required=True, type=Path)
    ap.add_argument("--bam", required=True, type=Path)
    ap.add_argument("--reads-mapping", required=True, type=Path,
                    help="CAMISIM's reads_mapping.tsv.gz for THIS sample")
    ap.add_argument("--contig-to-bin", action="append", default=[], metavar="LABEL=PATH",
                    help="repeatable; LABEL becomes AMBER's display label and its "
                         "genome/<label>/ subdirectory, matching the in-plan scorer's "
                         "own label derivation")
    ap.add_argument("--nfcore-contig-to-bin", type=Path, metavar="PATH",
                    help="nf-core/mag's consolidated GenomeBinning/contig_to_bin/"
                         "contig_to_bin_map.tsv; split into one label per `binner` value")
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--gold", type=Path,
                    help="reuse a previously built gold standard instead of rebuilding")
    ap.add_argument("--lib", type=Path,
                    default=Path(__file__).resolve().parents[2]
                    / "src/metasmith_libraries/resources/lib/cami_gold_standard.py",
                    help="the SAME vote script the in-plan transform runs")
    args = ap.parse_args()

    assert args.contig_to_bin or args.nfcore_contig_to_bin, (
        "give --nfcore-contig-to-bin (the reference arm's consolidated map) or at least one "
        "--contig-to-bin LABEL=PATH; there is nothing to score"
    )
    tables: dict[str, Path] = {}
    for spec in args.contig_to_bin:
        assert "=" in spec, f"--contig-to-bin wants LABEL=PATH, got [{spec}]"
        label, _, p = spec.partition("=")
        tables[label] = Path(p)

    needed = [args.assembly, args.bam, args.reads_mapping, args.lib, *tables.values()]
    if args.nfcore_contig_to_bin:
        needed.append(args.nfcore_contig_to_bin)
    for p in needed:
        assert p.exists(), f"missing input [{p}]"

    args.outdir.mkdir(parents=True, exist_ok=True)
    work = args.outdir / "work"
    work.mkdir(exist_ok=True)
    binds = [Path("/scratch/phyberos"), args.lib.resolve().parent, args.outdir.resolve()]
    binds += [p.resolve().parent for p in [args.assembly, args.bam, args.reads_mapping]]

    print("measuring contig lengths", file=sys.stderr)
    lengths_file = work / "contig_lengths.tsv"
    contigs = set(contig_lengths(args.assembly, lengths_file))
    print(f"  {len(contigs)} contigs", file=sys.stderr)

    gold = args.gold
    if gold is None:
        gold = args.outdir / "contig_gold_standard_table.tsv"
        read_contig = work / "read_contig.tsv"
        _run(
            f"samtools view -@ {args.threads} -F 0x904 {args.bam.resolve()} "
            f"| cut -f1,3 > {read_contig.resolve()}",
            image="samtools", binds=binds,
        )
        # Assert the BAM belongs to THIS assembly before the vote, not after: the vote's
        # own assert fires on zero overlap but reads as a mate-suffix problem, which is a
        # different and much rarer cause than a cross-mapped BAM.
        seen = set()
        with open(read_contig) as f:
            for i, line in enumerate(f):
                if i >= 100000:
                    break
                parts = line.rstrip("\n").split("\t")
                if len(parts) == 2:
                    seen.add(parts[1])
        overlap = len(seen & contigs)
        assert overlap > 0, (
            f"none of the {len(seen)} contig names in the first 100k BAM records occur in "
            f"[{args.assembly}]. This BAM is aligned against a DIFFERENT assembly -- "
            f"nf-core/mag only produces a self-mapped BAM under "
            f"binning_map_mode = 'own'."
        )
        print(f"  BAM/assembly contig overlap: {overlap}/{len(seen)} sampled",
              file=sys.stderr)
        _run(
            f"python {args.lib.resolve()} {read_contig.resolve()} "
            f"{args.reads_mapping.resolve()} {lengths_file.resolve()} "
            f"{args.sample_id} {gold.resolve()}",
            image="polars", binds=binds,
        )
    sid = sample_id_of(gold)
    print(f"gold standard @SampleID = [{sid}]", file=sys.stderr)

    # amber.py matches gold standard and prediction by @SampleID, so the prediction takes
    # the gold standard's own value -- exactly as amber.py's protocol does. Nothing ever
    # compares that string to an external identifier.
    predictions: dict[str, list[tuple[str, str]]] = {}
    if args.nfcore_contig_to_bin:
        predictions.update(read_nfcore_contig_to_bin(args.nfcore_contig_to_bin, contigs))
    for label, table in sorted(tables.items()):
        predictions[label] = _read_contig_to_bin(table, contigs)

    results: dict[str, Path] = {}
    for label, pairs in sorted(predictions.items()):
        assert pairs, f"no scorable rows for label [{label}]"
        pred = work / f"prediction.{label}.tsv"
        with open(pred, "w") as f:
            f.write(f"@Version:0.9.1\n@SampleID:{sid}\n\n@@SEQUENCEID\tBINID\n")
            for contig, b in pairs:
                f.write(f"{contig}\t{b}\n")

        amber_out = work / f"amber_out.{label}"
        if amber_out.exists():
            shutil.rmtree(amber_out)
        _run(
            f"amber.py -g {gold.resolve()} -l {label} -o {amber_out.resolve()} "
            f"--skip_gs {pred.resolve()}",
            image="amber", binds=binds,
        )
        src_results = amber_out / "results.tsv"
        src_bins = amber_out / "genome" / label / "metrics_per_bin.tsv"
        assert src_results.exists(), (
            f"amber.py wrote no results.tsv for [{label}] -- check {amber_out}/log.txt"
        )
        assert src_bins.exists(), (
            f"amber.py wrote no genome/{label}/metrics_per_bin.tsv for [{label}] -- "
            f"check {amber_out}/log.txt"
        )
        dst = args.outdir / label
        dst.mkdir(exist_ok=True)
        shutil.copyfile(src_results, dst / "results.tsv")
        shutil.copyfile(src_bins, dst / "metrics_per_bin.tsv")
        results[label] = dst
        print(f"  scored [{label}] -> {dst}", file=sys.stderr)

    print("\nAMBER outputs, same two files per label as the in-plan scorer emits:")
    for label, d in sorted(results.items()):
        print(f"  {label}: {d}/results.tsv  {d}/metrics_per_bin.tsv")
    print(f"gold standard: {gold}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
