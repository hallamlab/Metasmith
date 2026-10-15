# plot_metadata -- PLOT_METADATA (asv_pipeline.nf:3793), upstream's plot_metadata.py over a
# laid-out tree, grouped by the sheet's first label with no control subtraction.
#
# The script's mito block raises on a table with no mitochondrial ASV, and upstream passes
# --make-mito unconditionally, which only a host-associated study survives. Here the block
# runs only when the table has rows, and the three mito products are otherwise written
# header-only, so a consumer reads an empty table rather than finding nothing.
#
# analysis.min_level_size is applied last, to the tables the analyses read.

import yaml
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
fate    = model.AddRequirement(lib.GetType("aspire::read_fate"), parents={study})
clean   = model.AddRequirement(lib.GetType("aspire::counts_clean"), parents={study})
removed = model.AddRequirement(lib.GetType("aspire::counts_removed"), parents={study})
tax     = model.AddRequirement(lib.GetType("amplicon::asv_taxonomy"), parents={study})
params  = model.AddRequirement(lib.GetType("aspire::params"), parents={study})
md      = model.AddProduct(lib.GetType("aspire::analysis_metadata"))
am      = model.AddProduct(lib.GetType("aspire::analysis_asv_meta"))
counts  = model.AddProduct(lib.GetType("aspire::analysis_counts"))
md_mito = model.AddProduct(lib.GetType("aspire::metadata_mito"))
am_mito = model.AddProduct(lib.GetType("aspire::asv_meta_mito"))
af_mito = model.AddProduct(lib.GetType("aspire::asv_final_mito"))


def protocol(context: ExecutionContext):
    iscripts, istudy = context.Input(scripts), context.Input(study)
    outs = {p: context.Output(p) for p in (md, am, counts, md_mito, am_mito, af_mito)}
    header = istudy.local.read_text().splitlines()[0].split("\t")
    sid_col, label = header[0], header[1]
    min_level = yaml.safe_load(context.Input(params).local.read_text())["analysis"]["min_level_size"]
    L = "metadata_layout"

    produced = {
        md: f"{L}/metadata/metadata_updated_micro.tsv",
        am: f"{L}/metadata/ASV_meta_micro.tsv",
        counts: f"{L}/ASVs/ASV_final.micro.tsv",
        md_mito: f"{L}/mito/metadata/metadata_updated_mito.tsv",
        am_mito: f"{L}/mito/metadata/ASV_meta_mito.tsv",
        af_mito: f"{L}/mito/ASVs/ASV_final.mito.tsv",
    }
    micro_twin = {md_mito: md, am_mito: am, af_mito: counts}
    copies = ""
    for p, src in produced.items():
        dst = outs[p].container
        if p in micro_twin:
            twin = outs[micro_twin[p]].container
            copies += f'if [ -s {src} ]; then cp {src} {dst}; else head -n1 {twin} > {dst}; fi\n'
        else:
            copies += f"cp {src} {dst}\n"

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        python {iscripts.container}/upstream_layout.py metadata --sheet {istudy.container} \
            --fate {context.Input(fate).container} --label "{label}" \
            --clean {context.Input(clean).container} --removed {context.Input(removed).container} \
            --out {L}
        mito=""
        if [ "$(wc -l < {L}/mito/ASVs/ASV_target.mito.tsv)" -gt 1 ]; then mito=--make-mito; fi
        python {iscripts.container}/plot_metadata.py --data-dir $PWD/{L} --sub-dir . \
            --metadata $PWD/{L}/metadata.tsv --sample-manifest $PWD/{L}/manifest.tsv \
            --taxonomy {context.Input(tax).container} \
            --asv-micro $PWD/{L}/ASVs/ASV_target.micro.tsv \
            --asv-mito $PWD/{L}/mito/ASVs/ASV_target.mito.tsv \
            --fastq-stats $PWD/{L}/stats/fastq_stats.tsv \
            --sample-id-col "{sid_col}" --group1-col "{label}" --color-col Color \
            --subtraction-groups "" --make-micro $mito --verbose
        {copies}
        python {iscripts.container}/upstream_layout.py blank --sheet {istudy.container} \
            --min-level-size {min_level} {outs[md].container} {outs[am].container} \
            {outs[md_mito].container} {outs[am_mito].container}
        rm -r {L}
    """)

    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=all(o.local.exists() for o in outs.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=3),
    ),
)
