# denoise -- DENOISE (asv_pipeline.nf:3367), with CONCAT_FASTAS (3274), DEREPLICATE (3298),
# CHIMERA_CHECK (3393) and CREATE_COUNT_MATRIX (3417). Every read already carries its sample,
# from merge_and_filter_reads, so the group's order does not matter here.

import yaml
from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::vsearch.env"))
study    = model.AddRequirement(lib.GetType("aspire::study_metadata"))
meta     = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={study})
filtered = model.AddRequirement(lib.GetType("aspire::filtered_fasta"), parents={meta})
params   = model.AddRequirement(lib.GetType("aspire::params"), parents={study})
counts   = model.AddProduct(lib.GetType("amplicon::asv_table"))
seqs     = model.AddProduct(lib.GetType("amplicon::asv_seqs"))


def protocol(context: ExecutionContext):
    ifiltered = context.InputGroup(filtered)
    icounts, iseqs = context.Output(counts), context.Output(seqs)
    cfg = yaml.safe_load(context.Input(params).local.read_text())
    unoise, identity = cfg["unoise"], cfg["count"]["identity"]
    threads = context.params.get("cpus", 8)
    fastas = " ".join(str(f.container) for f in ifiltered)

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        gzip -cd {fastas} > concat.fasta
        vsearch --derep_fulllength concat.fasta --output derep.fasta --sizeout --threads {threads}
        vsearch --cluster_unoise derep.fasta --centroids centroids.fasta \
            --sizein --sizeout --relabel ASV --minsize {unoise['min_size']} \
            --unoise_alpha {unoise['alpha']} --threads {threads}
        vsearch --uchime3_denovo centroids.fasta --nonchimeras nochimeras.fasta --sizein
        vsearch --fastx_filter nochimeras.fasta --xsize --fastaout {iseqs.container}
        vsearch --usearch_global concat.fasta --db {iseqs.container} --id {identity} \
            --otutabout {icounts.container} --threads {threads}
        rm concat.fasta derep.fasta centroids.fasta nochimeras.fasta
    """)

    return ExecutionResult(
        manifest=[{counts: icounts.local, seqs: iseqs.local}],
        success=icounts.local.exists() and iseqs.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=6),
    ),
)
