from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::virsorter2.env"))
asm         = model.AddRequirement(lib.GetType("sequences::contig_batch"))
db          = model.AddRequirement(lib.GetType("annotation::virsorter2_db"))
out_seqs    = model.AddProduct(lib.GetType("annotation::virsorter2_viral_sequences"))
out_scores  = model.AddProduct(lib.GetType("annotation::virsorter2_scores"))
out_affi    = model.AddProduct(lib.GetType("annotation::virsorter2_affi_contigs"))
out_boundary = model.AddProduct(lib.GetType("annotation::virsorter2_boundary"))
out_calls   = model.AddProduct(lib.GetType("viromics::virsorter2_candidate_virus"))

# VirSorter2 states the trimmed viral interval in final-viral-boundary.tsv and
# repeats it in the record name it writes into the FASTA (`<contig>||full`,
# `<contig>||partial`). The boundary table is the one that carries coordinates,
# so the adapter reads it -- by column name, since the column set has moved
# between 2.2.x releases and an index would read the wrong field in silence.
MIN_LENGTH = 5000


def _any_contig_at_least(fasta: Path, n: int) -> bool:
    length = 0
    with open(fasta) as f:
        for line in f:
            if line.startswith(">"):
                if length >= n:
                    return True
                length = 0
            else:
                length += len(line.strip())
    return length >= n


# The headers a completed 2.2.4 run writes, for a batch that completed with nothing to report.
EMPTY_BOUNDARY_COLUMNS = (
    "seqname trim_orf_index_start trim_orf_index_end trim_bp_start trim_bp_end trim_pr trim_pr_max "
    "prox_orf_index_start prox_orf_index_end prox_bp_start prox_bp_end prox_pr prox_pr_max partial "
    "full_orf_index_start full_orf_index_end full_bp_start full_bp_end pr_full arc bac euk vir mix "
    "unaligned hallmark_cnt group shape seqname_new final_max_score final_max_score_group").split()
EMPTY_SCORE_COLUMNS = (
    "seqname dsDNAphage NCLDV RNA ssDNA lavidaviridae max_score max_score_group length hallmark "
    "viral cellular").split()

_SEQ   = ("seqname", "seqname_new")
_START = ("trim_bp_start", "full_bp_start")
_END   = ("trim_bp_end", "full_bp_end")
_SCORE = ("max_score", "trim_pr_max", "trim_pr", "pr_full")


def _first_present(col: dict, names, header):
    for n in names:
        if n in col: return col[n]
    assert False, (
        f"VirSorter2's final-viral-boundary.tsv has none of {names}; header was "
        f"{header}. The candidate_virus adapter resolves columns by name -- add "
        "the release's spelling here rather than falling back to an index."
    )


def _write_calls(boundary_tsv: Path, out: Path):
    with open(boundary_tsv) as f:
        head = f.readline().rstrip("\n")
        if not head:
            out.write_text("contig_id\tstart\tend\tcaller\tscore\n")
            return
        header = head.split("\t")
        col = {name: i for i, name in enumerate(header)}
        i_seq = _first_present(col, _SEQ, header)
        i_start = _first_present(col, _START, header)
        i_end = _first_present(col, _END, header)
        i_score = _first_present(col, _SCORE, header)
        with open(out, "w") as o:
            o.write("contig_id\tstart\tend\tcaller\tscore\n")
            for line in f:
                if not line.strip(): continue
                row = line.rstrip("\n").split("\t")
                name = row[i_seq].split("||")[0]
                o.write(f"{name}\t{row[i_start]}\t{row[i_end]}\tvirsorter2\t{row[i_score]}\n")


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    idb  = context.Input(db)
    iseqs = context.Output(out_seqs)
    iscores = context.Output(out_scores)
    iaffi = context.Output(out_affi)
    iboundary = context.Output(out_boundary)
    icalls = context.Output(out_calls)

    threads = context.params.get("cpus", 8)
    workdir = "vs2_out"

    # `--db-dir /db`, not `/db/db`. downloadVirsorter2DB renames `virsorter setup`'s
    # own output directory to the product, so the product IS the database: `group/`,
    # `hmm/` and `rbs/` sit at its top with no wrapping `db/`. A nested path here reads
    # as a setup that never ran.
    #
    # HOME points into the work directory rather than /tmp: virsorter writes a config
    # under $HOME/.virsorter, and on a host whose /tmp is a small shared tmpfs that is
    # the wrong place for it.

    if not _any_contig_at_least(Path(iasm.local), MIN_LENGTH):
        print(f"NOTICE: no contig in this batch reaches {MIN_LENGTH} bp; writing empty tables", flush=True)
        iboundary.local.write_text("\t".join(EMPTY_BOUNDARY_COLUMNS) + "\n")
        iscores.local.write_text("\t".join(EMPTY_SCORE_COLUMNS) + "\n")
        for o in (iseqs, iaffi):
            o.local.touch()
        _write_calls(iboundary.local, icalls.local)
        return ExecutionResult(
            manifest=[{out_seqs: iseqs.local, out_scores: iscores.local, out_affi: iaffi.local,
                       out_boundary: iboundary.local, out_calls: icalls.local}],
            success=True,
        )

    context.ExecWithEnv(
        env=image,
        binds=[(idb.external, "/db")],
        cmd=f"""
            export HOME="$PWD"

            virsorter run \
                --seqfile {iasm.container} \
                --db-dir /db \
                --working-dir {workdir} \
                --jobs {threads} \
                --include-groups dsDNAphage,NCLDV,RNA,ssDNA,lavidaviridae \
                --min-length {MIN_LENGTH} \
                --min-score 0.5 \
                --keep-original-seq \
                --prep-for-dramv \
                all \
                || {{ c=$?; case $c in 125|137) exit $c ;; esac; }}
        """,
    )

    context.LocalShell(
        f"cp {workdir}/final-viral-combined.fa {iseqs.local} 2>/dev/null || touch {iseqs.local}"
    )
    context.LocalShell(
        f"cp {workdir}/final-viral-score.tsv {iscores.local} 2>/dev/null || touch {iscores.local}"
    )
    context.LocalShell(
        f"cp {workdir}/for-dramv/viral-affi-contigs-for-dramv.tab {iaffi.local} 2>/dev/null || touch {iaffi.local}"
    )
    context.LocalShell(
        f"cp {workdir}/final-viral-boundary.tsv {iboundary.local} 2>/dev/null || touch {iboundary.local}"
    )

    # A batch of short contigs leaves nothing past VirSorter2's own length filter, and it stops at
    # combine_linear_circular with an error rather than reporting no viruses.
    if Path(workdir, "EARLY-EXIT").exists() and iboundary.local.stat().st_size == 0:
        print("NOTICE: no contig in this batch passed VirSorter2's preprocessing; writing empty tables", flush=True)
        iboundary.local.write_text("\t".join(EMPTY_BOUNDARY_COLUMNS) + "\n")
        iscores.local.write_text("\t".join(EMPTY_SCORE_COLUMNS) + "\n")
    context.LocalShell(f"rm -rf {workdir} 2>/dev/null || true")

    # Every copy above falls back to `touch`, so a run that died still leaves five files
    # and an exit code of zero. The boundary table is the one that cannot be empty: a
    # completed run writes it with a header even when nothing scored, so an empty one
    # means the tool never got there.
    assert iboundary.local.stat().st_size > 0, (
        "virsorter wrote no final-viral-boundary.tsv; the run did not complete"
    )
    _write_calls(iboundary.local, icalls.local)

    return ExecutionResult(
        manifest=[
            {
                out_seqs: iseqs.local,
                out_scores: iscores.local,
                out_affi: iaffi.local,
                out_boundary: iboundary.local,
                out_calls: icalls.local,
            },
        ],
        success=icalls.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=8,
        memory=Size.GB(24),
        duration=Duration(hours=12),
    ),
)
