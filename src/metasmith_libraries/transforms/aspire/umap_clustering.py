# umap_clustering -- UMAP_CLUSTERING (asv_pipeline.nf:4055), once per label of the sample
# sheet with at least two levels, at upstream's config defaults. Reads the long-form table
# only, as upstream does. The secondary column is the next such label, or the label itself
# when it is the only one, as the .nf's default `Month` names a column only the lung study has.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
survey  = model.AddRequirement(lib.GetType("amplicon::survey"))
am      = model.AddRequirement(lib.GetType("aspire::analysis_asv_meta"), parents={survey})
out     = model.AddProduct(lib.GetType("aspire::umap_plots"))


def protocol(context: ExecutionContext):
    iout, s = context.Output(out), context.Input(scripts).container
    o = iout.container
    sid_col = context.Input(survey).local.read_text().splitlines()[0].split("\t")[0]

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl NUMBA_CACHE_DIR=$PWD/.cache/numba
        mkdir -p {o}
        python {s}/upstream_layout.py labels --sheet {context.Input(survey).container} \
            --table {context.Input(am).container} --skipped {o}/skipped_labels.tsv > labels.tsv
        while IFS=$'\\t' read -r L D L2; do
            mkdir -p {o}/$D
            python {s}/upstream_layout.py recolor --label "$L" {context.Input(am).container} am.tsv
            python {s}/umap_clustering.py --input am.tsv --output-prefix {o}/$D/umap_clustering \
                --count-col count --sample-col "{sid_col}" --group1-col "$L" --color-col Color \
                --group2-col "$L2" --formats pdf,png,svg --normalize clr --transform sqrt \
                --n-neighbors 15 --min-dist 0.1 --umap-metric euclidean \
                --min-cluster-size 10 --min-samples 5 --hdbscan-metric euclidean --random-state 42
        done < labels.tsv
        rm -f labels.tsv am.tsv
        rm -rf .cache
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / "skipped_labels.tsv").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=survey,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
