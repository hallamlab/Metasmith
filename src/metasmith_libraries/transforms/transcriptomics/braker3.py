from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={exp})
bam     = model.AddRequirement(lib.GetType("transcriptomics::merged_bam"), parents={exp})
image   = model.AddRequirement(lib.GetType("env::braker3.env"))
out_gff = model.AddProduct(lib.GetType("transcriptomics::braker3_gff"))
out_aa  = model.AddProduct(lib.GetType("transcriptomics::braker3_proteins"))

def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    ibam = context.Input(bam)
    igff = context.Output(out_gff)
    iaa  = context.Output(out_aa)
    threads = context.params.get('cpus')
    threads = 16 if threads is None else threads

    # Augustus config is read-only in the container; copy to writable workdir.
    # Pre-clean assembly headers: braker3 replaces spaces with underscores in
    # genome.fa headers (full description) but bam2hints uses original BAM
    # contig names (accession only), causing filterIntronsFindStrand.pl to find
    # no matching sequences and produce an empty hints file.  Truncating headers
    # at the first whitespace before passing to braker3 prevents this mismatch.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            cp -r $AUGUSTUS_CONFIG_PATH augustus_config
            awk '/^>/{{print substr($1,1); next}}{{print}}' {iasm.container} > genome_clean.fa
            braker.pl \
                --genome=genome_clean.fa \
                --bam={ibam.container} \
                --species=porphyridium_run_$$ \
                --threads={threads} \
                --gff3 \
                --workingdir=braker_out \
                --softmasking \
                --AUGUSTUS_CONFIG_PATH=$(pwd)/augustus_config
        """,
    )

    context.LocalShell(f"cp braker_out/braker.gff3 {igff.local}")
    context.LocalShell(f"cp braker_out/braker.aa {iaa.local}")

    return ExecutionResult(
        manifest=[{out_gff: igff.local, out_aa: iaa.local}],
        success=igff.local.exists() and iaa.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=16,
        memory=Size.GB(64),
        duration=Duration(hours=48),
    ),
)
