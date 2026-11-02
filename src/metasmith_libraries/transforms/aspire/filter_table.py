# filter_table -- FILTER_TABLE (asv_pipeline.nf:3444), upstream's filter_ASV_table.py unchanged.

import yaml
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
counts  = model.AddRequirement(lib.GetType("amplicon::asv_table"), parents={study})
seqs    = model.AddRequirement(lib.GetType("amplicon::asv_seqs"), parents={study})
params  = model.AddRequirement(lib.GetType("aspire::params"), parents={study})
fcounts = model.AddProduct(lib.GetType("aspire::asv_filtered_counts"))
fseqs   = model.AddProduct(lib.GetType("aspire::asv_filtered_seqs"))


def protocol(context: ExecutionContext):
    ifcounts, ifseqs = context.Output(fcounts), context.Output(fseqs)
    cfg = yaml.safe_load(context.Input(params).local.read_text())["table_filter"]
    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        python {context.Input(scripts).container}/filter_ASV_table.py \
            {context.Input(counts).container} {ifcounts.container} \
            {cfg['min_sample_sum']} {cfg['min_asv_sum']} \
            {context.Input(seqs).container} ASVs_filtered.fasta
        gzip -n -c ASVs_filtered.fasta > {ifseqs.container}
        rm ASVs_filtered.fasta
    """)

    return ExecutionResult(
        manifest=[{fcounts: ifcounts.local, fseqs: ifseqs.local}],
        success=ifcounts.local.exists() and ifseqs.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
