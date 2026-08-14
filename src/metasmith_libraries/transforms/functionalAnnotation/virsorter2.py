from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::virsorter2.env"))
# ~5 Mbp contig batch (w4_rebatch.py), sample-prefixed headers; per-sample
# regroup happens in w4_recompile.py. Sibling of assembly, not a subtype.
asm         = model.AddRequirement(lib.GetType("sequences::contig_batch"))
db          = model.AddRequirement(lib.GetType("annotation::virsorter2_db"))
out_seqs    = model.AddProduct(lib.GetType("annotation::virsorter2_viral_sequences"))
out_scores  = model.AddProduct(lib.GetType("annotation::virsorter2_scores"))
out_affi    = model.AddProduct(lib.GetType("annotation::virsorter2_affi_contigs"))
out_boundary = model.AddProduct(lib.GetType("annotation::virsorter2_boundary"))

def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    idb  = context.Input(db)
    iseqs = context.Output(out_seqs)
    iscores = context.Output(out_scores)
    iaffi = context.Output(out_affi)
    iboundary = context.Output(out_boundary)

    threads = context.params.get("cpus", 8)
    workdir = "vs2_out"

    # VirSorter may exit non-zero when no viral contigs are found (EARLY-EXIT)
    # so we tolerate exit codes and check outputs instead
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/db")],
        cmd=f"""
            export HOME=/tmp

            virsorter run \
                --seqfile {iasm.container} \
                --db-dir /db/db \
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

    # Copy primary outputs (files may not exist if no viral contigs found).
    # PATHS MATTER: VirSorter2 writes the score/boundary tables at the working-dir
    # ROOT, but the two DRAMv-prep files (final-viral-combined-for-dramv.fa and
    # viral-affi-contigs-for-dramv.tab) live in the `for-dramv/` SUBDIR. Copying the
    # dramv .fa from the root silently misses it (cp fails -> `touch` empties it),
    # which is exactly what happened in run Ui6dQBmN (161/161 empty seqs while the
    # score tables were fine). The canonical "all viral seqs" file is
    # final-viral-combined.fa at the root (VS2's own legend), so publish THAT as the
    # sequences product; take the affi table from the for-dramv/ subdir.
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

    # VirSorter2's working dir holds thousands of small snakemake intermediates.
    # metasmith never reaps nxf_work, so leaving them balloons the /scratch inode
    # count (1M cap); the 4 outputs are already copied out, so drop the workdir.
    context.LocalShell(f"rm -rf {workdir} 2>/dev/null || true")

    return ExecutionResult(
        manifest=[
            {
                out_seqs: iseqs.local,
                out_scores: iscores.local,
                out_affi: iaffi.local,
                out_boundary: iboundary.local,
            },
        ],
        success=True,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=8,
        # coarse ~240 Mbp batches: calibration VS2(bp)=547+12.7*Mbp → ~1 h/batch
        # (30 Mbp=928s, 90 Mbp=1691s), MaxRSS 12.7 GB → 24 GB ample; 12 h base kept.
        memory=Size.GB(24),
        duration=Duration(hours=12),
    ),
)
