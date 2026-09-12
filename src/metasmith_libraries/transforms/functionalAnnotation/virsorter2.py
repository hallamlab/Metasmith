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
                --min-length 5000 \
                --min-score 0.5 \
                --keep-original-seq \
                --prep-for-dramv \
                all \
                || true
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
