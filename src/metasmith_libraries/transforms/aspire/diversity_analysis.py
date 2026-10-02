# diversity_analysis -- DIVERSITY_ANALYSIS (asv_pipeline.nf:4399) without its mito branch,
# which is diversity_mito. Shannon, Bray-Curtis and Jaccard are computed once at the top of
# the product; the plots and PERMANOVA run once per label with at least two levels, at
# upstream's config defaults. Each run drops the samples its label leaves empty. The secondary
# column is the next such label when every remaining sample has a value for it, as the .nf's
# default `Month` names a column only the lung study has.
#
# This row reads no params, so a count table from outside ASPIRE can reach it.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
survey  = model.AddRequirement(lib.GetType("amplicon::survey"))
counts  = model.AddRequirement(lib.GetType("amplicon::abundance_table"), parents={survey})
md      = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={survey})
out     = model.AddProduct(lib.GetType("aspire::diversity_outputs"))


def protocol(context: ExecutionContext):
    iout, s = context.Output(out), context.Input(scripts).container
    o = iout.container
    sid_col = context.Input(survey).local.read_text().splitlines()[0].split("\t")[0]

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl NUMBA_CACHE_DIR=$PWD/.cache/numba
        mkdir -p {o}
        python {s}/calc_div.py --micro-table {context.Input(counts).container} --outdir {o}
        python {s}/upstream_layout.py labels --sheet {context.Input(survey).container} \
            --table {context.Input(md).container} --skipped {o}/skipped_labels.tsv > labels.tsv
        while IFS=$'\\t' read -r L D L2; do
            L2=$(python {s}/upstream_layout.py recolor --label "$L" --drop-unlabelled --secondary "$L2" \
                {context.Input(md).container} md.tsv)
            sec=()
            [ -n "$L2" ] && sec=(--secondary-col "$L2")
            python {s}/plot_diversity.py --metadata md.tsv --sample-col "{sid_col}" \
                --group-col "$L" --color-col Color "${{sec[@]}}" \
                --alpha-table {o}/shannon.tsv --distance-bray {o}/bray.tsv \
                --distance-jaccard {o}/jaccard.tsv --output-dir {o}/$D \
                --umap-neighbors 30 --umap-min-dist 0.01 --permanova-perms 999 \
                --random-state 42 --verbose
        done < labels.tsv
        rm -f labels.tsv md.tsv
        rm -rf .cache
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / "shannon.tsv").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=survey,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=3),
    ),
)
