"""Map ASV sequences to contigs using BLASTn with configurable identity threshold."""

from metasmith.python_api import *

lib       = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model     = Transform()
image     = model.AddRequirement(lib.GetType("env::blast.env"))
asvs      = model.AddRequirement(lib.GetType("amplicon::asv_seqs"))
contigs   = model.AddRequirement(lib.GetType("sequences::assembly"))
threshold = model.AddRequirement(lib.GetType("amplicon::blast_identity_threshold"))
hits      = model.AddProduct(lib.GetType("amplicon::asv_contig_map"))

def protocol(context: ExecutionContext):
    iasvs      = context.Input(asvs)
    icontigs   = context.Input(contigs)
    ithreshold = context.Input(threshold)
    ihits      = context.Output(hits)

    # Read threshold value from the input file
    pct_identity = ithreshold.local.read_text().strip()

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            makeblastdb \
                -in {icontigs.container} \
                -dbtype nucl \
                -out contig_db
            blastn \
                -query {iasvs.container} \
                -db contig_db \
                -perc_identity {pct_identity} \
                -qcov_hsp_perc 90 \
                -outfmt 6 \
                -max_target_seqs 10000 \
                -out {ihits.container}
        """,
    )

    return ExecutionResult(
        manifest=[
            {
                hits: ihits.local,
            },
        ],
        success=ihits.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asvs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    )
)
