# spieceasi -- SPIECEASI (asv_pipeline.nf:5452) at upstream's config defaults.
#
# Upstream force-keeps the ASVs significant in its first indicator group's summary. Here every
# label's summary contributes, so an ASV that marks any label is kept regardless of the
# prevalence and abundance filters.
#
# This row reads no params, so a count table from outside ASPIRE can reach it.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
survey  = model.AddRequirement(lib.GetType("amplicon::survey"))
policy  = model.AddRequirement(lib.GetType("aspire::spieceasi_on"), parents={survey})
counts  = model.AddRequirement(lib.GetType("amplicon::abundance_table"), parents={survey})
keep    = model.AddRequirement(lib.GetType("aspire::indicspecies_results"), parents={survey})
all     = model.AddProduct(lib.GetType("aspire::network_graph_all"))
thr     = model.AddProduct(lib.GetType("aspire::network_graph_thr"))
nf      = model.AddProduct(lib.GetType("aspire::network_node_features"))


def protocol(context: ExecutionContext):
    outs = {p: context.Output(p) for p in (all, thr, nf)}
    s = context.Input(scripts).container
    cpus = context.params.get("cpus", 16)

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        python {s}/upstream_layout.py force_keep --results {context.Input(keep).container} \
            --out force_keep.tsv
        Rscript {s}/run_spieceasi.R --counts {context.Input(counts).container} --outdir net \
            --prefix spieceasi --transpose TRUE --min-rel-abund 0 --min-prevalence 0.25 \
            --force-keep-asvs force_keep.tsv --remove-zero-var TRUE --method glasso \
            --lambda-min-ratio 0.1 --nlambda 20 --rep-num 50 --thresh 0.1 \
            --pulsar-criterion bstars --ncores {cpus} --seed 10010 --edge-threshold 0.1 \
            --keep-negative TRUE --layout-iters 1000 --force-filter FALSE \
            --force-spieceasi FALSE --force-graphs TRUE
        cp net/spieceasi_network_pos_all.graphml {outs[all].container}
        cp net/spieceasi_network_pos_thr.graphml {outs[thr].container}
        cp net/spieceasi_node_features.csv {outs[nf].container}
        rm -r net force_keep.tsv
    """)

    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=not [o for o in outs.values() if not o.local.exists()],
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=survey,
    resources=Resources(
        cpus=16,
        memory=Size.GB(64),
        duration=Duration(hours=24),
    ),
)
