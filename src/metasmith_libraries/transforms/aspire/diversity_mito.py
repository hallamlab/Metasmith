# diversity_mito -- DIVERSITY_ANALYSIS's run_mito branch (asv_pipeline.nf:4399), split out so
# the generic row stays reachable from any count table. The mitochondrial counts are the
# removed counts whose reason is `mitochondrial`.
#
# Upstream's --mito-mode reruns the micro analysis and then calls the same pipeline on the
# mito tables, so this calls it on the mito tables alone. A study with fewer than two
# mitochondrial ASVs or samples carrying them has nothing to ordinate: the product then
# holds only the mito table and a note saying why.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
removed = model.AddRequirement(lib.GetType("aspire::counts_removed"), parents={study})
md      = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={study})
out     = model.AddProduct(lib.GetType("aspire::diversity_mito_outputs"))


def protocol(context: ExecutionContext):
    iout, s = context.Output(out), context.Input(scripts).container
    o = iout.container
    sid_col = context.Input(study).local.read_text().splitlines()[0].split("\t")[0]

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl NUMBA_CACHE_DIR=$PWD/.cache/numba
        mkdir -p {o}
        n=$(python {s}/upstream_layout.py mito --removed {context.Input(removed).container} \
            --out {o}/ASV_target.mito.tsv)
        if [ "$n" -lt 2 ]; then
            echo "$n mitochondrial ASV(s): nothing to ordinate" > {o}/NOTE.txt
            exit 0
        fi
        python {s}/calc_div.py --mito-table {o}/ASV_target.mito.tsv --mito-outdir {o}
        python {s}/upstream_layout.py labels --sheet {context.Input(study).container} \
            --table {context.Input(md).container} --skipped {o}/skipped_labels.tsv > labels.tsv
        while IFS=$'\\t' read -r L D L2; do
            sec=()
            [ "$L2" != "$L" ] && sec=(--secondary-col "$L2")
            python {s}/upstream_layout.py recolor --label "$L" {context.Input(md).container} md.tsv
            python {s}/plot_diversity.py --metadata md.tsv --sample-col "{sid_col}" \
                --group-col "$L" --color-col Color "${{sec[@]}}" \
                --alpha-table {o}/shannon.mito.tsv --distance-bray {o}/bray.mito.tsv \
                --distance-jaccard {o}/jaccard.mito.tsv --output-dir {o}/$D \
                --umap-neighbors 30 --umap-min-dist 0.01 --permanova-perms 999 \
                --random-state 42 --verbose
        done < labels.tsv
        rm -f labels.tsv md.tsv
        rm -rf .cache
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / "ASV_target.mito.tsv").exists(),
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
