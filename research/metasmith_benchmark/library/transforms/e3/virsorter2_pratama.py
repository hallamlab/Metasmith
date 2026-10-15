# Pratama's per-sample VirSorter2 (reproduction_map B4):
# `--include-groups dsDNAphage,ssDNA --keep-original-seq --min-score 0.5 --min-length 5000`.
# No --prep-for-dramv here. Pratama runs that pass on the >=10 kb vOTUs, in dramv_votus_pratama.
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image        = model.AddRequirement(lib.GetType("env::virsorter2.env"))
asm          = model.AddRequirement(lib.GetType("sequences::contig_batch"))
db           = model.AddRequirement(lib.GetType("annotation::virsorter2_db"))
out_seqs     = model.AddProduct(lib.GetType("annotation::virsorter2_viral_sequences"))
out_scores   = model.AddProduct(lib.GetType("annotation::virsorter2_scores"))
out_boundary = model.AddProduct(lib.GetType("annotation::virsorter2_boundary"))
out_calls    = model.AddProduct(lib.GetType("viromics::virsorter2_candidate_virus"))

GROUPS = "dsDNAphage,ssDNA"
MIN_LENGTH = 5000


def _contig_lengths(fasta: Path) -> dict:
    lengths, contig_id, length = {}, None, 0
    with open(fasta) as f:
        for line in f:
            if line.startswith(">"):
                if contig_id is not None:
                    lengths[contig_id] = length
                contig_id, length = line[1:].strip().split()[0], 0
            else:
                length += len(line.strip())
    if contig_id is not None:
        lengths[contig_id] = length
    return lengths

# seqname/seqname_new is the only column the boundary table's own docs disagree on across
# 2.2.x releases; full_bp_start/full_bp_end are stable, and trim_bp_* is never read here --
# see _row_interval for why.
_SEQ        = ("seqname", "seqname_new")
_FULL_START = ("full_bp_start",)
_FULL_END   = ("full_bp_end",)
_SCORE      = ("max_score", "trim_pr_max", "trim_pr", "pr_full")
CALLS_HEADER = "contig_id\tstart\tend\tcaller\tscore\n"

# A sequence VirSorter2 kept whole (`--keep-original-seq`) carries one of these two suffixes.
_FULL_SEQ_SUFFIXES = ("full", "lt2gene")


def _first_present(col, names, header):
    for n in names:
        if n in col:
            return col[n]
    raise AssertionError(f"final-viral-boundary.tsv has none of {names}; header was {header}")


# contig_id, start, end (1-based inclusive) for one final-viral-boundary.tsv row.
# `--keep-original-seq` means a ||full/||lt2gene row IS the untrimmed contig, so its end is
# the contig's real length, not whatever the table's own boundary columns say -- those
# describe the hallmark region VirSorter2 found, which can be shorter than the sequence it
# chose to keep whole. A partial row's region is real: full_bp_start/end, the pre-trim
# boundary. trim_bp_* is never read: it is CheckV-style host trimming, which this
# reproduction step does not apply (that trimming is Pratama's later step, elsewhere).
def _row_interval(row: dict, contig_lengths: dict) -> tuple:
    seqname = row["seqname"]
    contig_id, _, suffix = seqname.partition("||")
    if suffix in _FULL_SEQ_SUFFIXES:
        assert contig_id in contig_lengths, f"{seqname!r}: {contig_id!r} is not a contig in this batch"
        return contig_id, 1, contig_lengths[contig_id]
    return contig_id, int(row["full_bp_start"]), int(row["full_bp_end"])


def _write_calls(boundary_tsv: Path, contig_lengths: dict, out: Path):
    with open(boundary_tsv) as f:
        head = f.readline().rstrip("\n")
        if not head:
            out.write_text(CALLS_HEADER)
            return
        header = head.split("\t")
        col = {name: i for i, name in enumerate(header)}
        i_seq   = _first_present(col, _SEQ, header)
        i_start = _first_present(col, _FULL_START, header)
        i_end   = _first_present(col, _FULL_END, header)
        i_score = _first_present(col, _SCORE, header)
        with open(out, "w") as o:
            o.write(CALLS_HEADER)
            for line in f:
                if not line.strip():
                    continue
                cells = line.rstrip("\n").split("\t")
                row = {"seqname": cells[i_seq], "full_bp_start": cells[i_start], "full_bp_end": cells[i_end]}
                contig_id, start, end = _row_interval(row, contig_lengths)
                o.write(f"{contig_id}\t{start}\t{end}\tvirsorter2\t{cells[i_score]}\n")


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    idb = context.Input(db)
    outs = {p: context.Output(p) for p in (out_seqs, out_scores, out_boundary, out_calls)}
    threads = context.params.get("cpus", 8)

    # `--db-dir /db`: the staged product IS the database, with group/, hmm/ and rbs/ at its top.
    context.ExecWithEnv(env=image, binds=[(idb.external, "/db")], cmd=f"""
        export HOME="$PWD"
        virsorter run --seqfile {iasm.container} --db-dir /db --working-dir vs2_out --jobs {threads} \
            --include-groups {GROUPS} --keep-original-seq --min-score 0.5 --min-length {MIN_LENGTH} all || true
    """)
    for product, name in ((out_seqs, "final-viral-combined.fa"), (out_scores, "final-viral-score.tsv"),
                          (out_boundary, "final-viral-boundary.tsv")):
        context.LocalShell(f"cp vs2_out/{name} {outs[product].local} 2>/dev/null || touch {outs[product].local}")
    context.LocalShell("rm -rf vs2_out")

    contig_lengths = _contig_lengths(iasm.local)

    # A completed run writes the boundary table with a header even when nothing scored. A batch with no
    # contig long enough to score is an empty result, and failing it would drop the merge after retries.
    if outs[out_boundary].local.stat().st_size == 0:
        longest = max(contig_lengths.values(), default=0)
        assert longest < MIN_LENGTH, f"virsorter wrote no final-viral-boundary.tsv on a batch with a {longest} bp contig"
        Log.Info(f"longest contig {longest} bp, under {MIN_LENGTH}: no VirSorter2 calls in this batch")
    _write_calls(outs[out_boundary].local, contig_lengths, outs[out_calls].local)
    return ExecutionResult(manifest=[{p: o.local for p, o in outs.items()}],
                           success=outs[out_calls].local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=8, memory=Size.GB(24), duration=Duration(hours=12)),
)
