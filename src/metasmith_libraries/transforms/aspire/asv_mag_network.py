# asv_mag_network -- ASV_MAG_NETWORK (asv_pipeline.nf:5654) at upstream's config defaults, over
# the unthresholded graph and with no MAG abundance table: the MAGs need not come from the
# amplicon samples, so there is no per-sample MAG abundance to correlate.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
graph   = model.AddRequirement(lib.GetType("aspire::network_graph_all"), parents={study})
nf      = model.AddRequirement(lib.GetType("aspire::network_node_features"), parents={study})
tax     = model.AddRequirement(lib.GetType("amplicon::asv_taxonomy"), parents={study})
counts  = model.AddRequirement(lib.GetType("aspire::analysis_counts"), parents={study})
pairing = model.AddRequirement(lib.GetType("aspire::asv_mag_pairing"), parents={study})
magl    = model.AddRequirement(lib.GetType("aspire::asv_mag_outputs"), parents={study})
out     = model.AddProduct(lib.GetType("aspire::asv_mag_network_outputs"))


def protocol(context: ExecutionContext):
    iout, s = context.Output(out), context.Input(scripts).container
    m = context.Input(magl).container

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl
        python {s}/asv_mag_network.py --graph {context.Input(graph).container} \
            --node-features {context.Input(nf).container} \
            --asv-mag-pairing {context.Input(pairing).container} \
            --taxonomy {context.Input(tax).container} --asv-counts {context.Input(counts).container} \
            --genome-summary {m}/tables/asv2mag_genome_summary.tsv \
            --reference-catalog {m}/references/barrnap_16s_reference_catalog.tsv \
            --outdir {iout.container} --prefix asv_mag_network \
            --asv-taxonomy-source ncbi --mag-taxonomy-source gtdb --mag-id-mode exact \
            --mag-abundance-format auto --mag-abundance-genome-col genome_id \
            --mag-abundance-sample-col sample_id --mag-abundance-value-col read_count \
            --min-shared-samples 5 --abundance-transform log1p \
            --functional-module-min-fraction 0.5 --min-pident 99.5 --min-qcov 100
        rm -rf .cache
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.is_dir() and any(iout.local.iterdir()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
