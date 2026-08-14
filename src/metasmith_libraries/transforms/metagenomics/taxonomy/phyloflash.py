from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::phyloflash.env"))
img_bb  = model.AddRequirement(lib.GetType("env::bbtools.env"))
db      = model.AddRequirement(lib.GetType("ref::phyloflash_db"))
reads   = model.AddRequirement(lib.GetType("sequences::short_reads"))

html    = model.AddProduct(lib.GetType("taxonomy::phyloflash_report_html"))
summary = model.AddProduct(lib.GetType("taxonomy::phyloflash_summary"))
ntu     = model.AddProduct(lib.GetType("taxonomy::phyloflash_ntu_abundance"))
ssu     = model.AddProduct(lib.GetType("taxonomy::phyloflash_ssu_spades"))

def protocol(context: ExecutionContext):
    idb      = context.Input(db)
    ireads   = context.Input(reads)
    ihtml    = context.Output(html)
    isumm    = context.Output(summary)
    intu     = context.Output(ntu)
    issu     = context.Output(ssu)

    threads  = context.params.get('cpus')
    threads_arg = "" if threads is None else f"-CPUs {threads}"

    context.ExecWithEnv().ifContainerDo(
        env=img_bb,
        cmd=f"""
            reformat.sh in={ireads.container} \
                out1=split_r1.fq.gz out2=split_r2.fq.gz
        """
    )

    # phyloFlash writes <lib>.* into CWD; use a stable -lib prefix then
    # move the four products. SPAdes is default-on, EMIRGE default-off in
    # v3.4 — no flag needed to skip it. -html requests the HTML report.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            phyloFlash.pl -lib pf_out \
                -read1 split_r1.fq.gz -read2 split_r2.fq.gz \
                -dbhome {idb.container} {threads_arg} \
                -html
            mv pf_out.phyloFlash.html             {ihtml.container}
            mv pf_out.phyloFlash.report.csv       {isumm.container}
            mv pf_out.phyloFlash.NTUabundance.csv {intu.container}
            # SPAdes runs only when phyloFlash recovers enough SSU reads.
            # Sparse samples (e.g. bbduk discards) skip it — emit an empty
            # marker so downstream curation has a uniform product set.
            if [ -f pf_out.spades_rRNAs.final.fasta ]; then
                mv pf_out.spades_rRNAs.final.fasta {issu.container}
            else
                : > {issu.container}
            fi
        """
    )

    return ExecutionResult(
        manifest=[{
            html:    ihtml.local,
            summary: isumm.local,
            ntu:     intu.local,
            ssu:     issu.local,
        }],
        success=isumm.local.exists() and intu.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=2),
    )
)
