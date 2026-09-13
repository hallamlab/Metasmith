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


def _longest_contig(fasta: Path) -> int:
    longest = current = 0
    with open(fasta) as f:
        for line in f:
            if line.startswith(">"):
                longest, current = max(longest, current), 0
            else:
                current += len(line.strip())
    return max(longest, current)

# The boundary table's column set moved between 2.2.x releases, so resolve by name.
_SEQ   = ("seqname", "seqname_new")
_START = ("trim_bp_start", "full_bp_start")
_END   = ("trim_bp_end", "full_bp_end")
_SCORE = ("max_score", "trim_pr_max", "trim_pr", "pr_full")
CALLS_HEADER = "contig_id\tstart\tend\tcaller\tscore\n"


def _first_present(col, names, header):
    for n in names:
        if n in col:
            return col[n]
    raise AssertionError(f"final-viral-boundary.tsv has none of {names}; header was {header}")


def _write_calls(boundary_tsv: Path, out: Path):
    with open(boundary_tsv) as f:
        head = f.readline().rstrip("\n")
        if not head:
            out.write_text(CALLS_HEADER)
            return
        header = head.split("\t")
        col = {name: i for i, name in enumerate(header)}
        i_seq, i_start, i_end, i_score = (_first_present(col, names, header) for names in (_SEQ, _START, _END, _SCORE))
        with open(out, "w") as o:
            o.write(CALLS_HEADER)
            for line in f:
                if not line.strip():
                    continue
                row = line.rstrip("\n").split("\t")
                o.write(f"{row[i_seq].split('||')[0]}\t{row[i_start]}\t{row[i_end]}\tvirsorter2\t{row[i_score]}\n")


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

    # A completed run writes the boundary table with a header even when nothing scored. A batch with no
    # contig long enough to score is an empty result, and failing it would drop the merge after retries.
    if outs[out_boundary].local.stat().st_size == 0:
        longest = _longest_contig(iasm.local)
        assert longest < MIN_LENGTH, f"virsorter wrote no final-viral-boundary.tsv on a batch with a {longest} bp contig"
        Log.Info(f"longest contig {longest} bp, under {MIN_LENGTH}: no VirSorter2 calls in this batch")
    _write_calls(outs[out_boundary].local, outs[out_calls].local)
    return ExecutionResult(manifest=[{p: o.local for p, o in outs.items()}],
                           success=outs[out_calls].local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=8, memory=Size.GB(24), duration=Duration(hours=12)),
)
