# module_mag_anchors -- MODULE_MAG_ANCHORS (asv_pipeline.nf:5705) at upstream's config defaults.
# The module stability stats are graph_network's, read from its outputs when it wrote them.

from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts  = model.AddRequirement(lib.GetType("lib::aspire"))
study    = model.AddRequirement(lib.GetType("aspire::study_metadata"))
policy   = model.AddRequirement(lib.GetType("aspire::asv_mag_link_on"), parents={study})
mall     = model.AddRequirement(lib.GetType("aspire::network_modules_all"), parents={study})
nf       = model.AddRequirement(lib.GetType("aspire::network_node_features"), parents={study})
tax      = model.AddRequirement(lib.GetType("amplicon::asv_taxonomy"), parents={study})
counts   = model.AddRequirement(lib.GetType("aspire::analysis_counts"), parents={study})
md       = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={study})
pairing  = model.AddRequirement(lib.GetType("aspire::asv_mag_pairing"), parents={study})
net      = model.AddRequirement(lib.GetType("aspire::network_outputs"), parents={study})
anchors  = model.AddProduct(lib.GetType("aspire::module_asv_anchor_table"))
summary  = model.AddProduct(lib.GetType("aspire::module_mag_anchor_summary"))
scores   = model.AddProduct(lib.GetType("aspire::sample_module_scores"))
top      = model.AddProduct(lib.GetType("aspire::sample_top_modules"))
matrix   = model.AddProduct(lib.GetType("aspire::sample_module_matrix"))
heatmaps = model.AddProduct(lib.GetType("aspire::sample_module_heatmaps"))

FILES = {
    anchors: "module_asv_anchor_table.tsv",
    summary: "module_mag_anchor_summary.tsv",
    scores: "sample_module_scores.tsv",
    top: "sample_top_modules.tsv",
    matrix: "sample_module_score_matrix.tsv",
}


def protocol(context: ExecutionContext):
    outs = {p: context.Output(p) for p in (*FILES, heatmaps)}
    s, n = context.Input(scripts).container, context.Input(net).container
    sid_col = context.Input(study).local.read_text().splitlines()[0].split("\t")[0]
    copies = "; ".join(f"cp res/{name} {outs[p].container}" for p, name in FILES.items())

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl
        stats=()
        [ -f {n}/network_modules_best_stats_all.tsv ] && stats=(--best-stats {n}/network_modules_best_stats_all.tsv)
        python {s}/summarize_module_mag_anchors.py --modules {context.Input(mall).container} \
            --node-features {context.Input(nf).container} --taxonomy {context.Input(tax).container} \
            --asv-mag-pairing {context.Input(pairing).container} \
            --asv-counts {context.Input(counts).container} --metadata {context.Input(md).container} \
            --sample-col "{sid_col}" --sample-code-col sample_code "${{stats[@]}}" --outdir res
        {copies}
        mkdir -p {outs[heatmaps].container}
        cp res/sample_module_score_heatmap.* {outs[heatmaps].container}/ 2>/dev/null || true
        rm -rf res .cache
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
        duration=Duration(hours=4),
    ),
)
