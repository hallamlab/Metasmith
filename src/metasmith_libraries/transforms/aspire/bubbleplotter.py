# bubbleplotter -- BUBBLEPLOTTER (asv_pipeline.nf:4018), once per label of the sample sheet
# with at least two levels, at upstream's config defaults. The script facets by two columns;
# the second is the next such label, or the label itself when it is the only one, as the
# .nf's default `Month` names a column only the lung study has.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
am      = model.AddRequirement(lib.GetType("aspire::analysis_asv_meta"), parents={study})
out     = model.AddProduct(lib.GetType("aspire::bubble_plots"))


def protocol(context: ExecutionContext):
    iout, s = context.Output(out), context.Input(scripts).container
    o = iout.container
    sid_col = context.Input(study).local.read_text().splitlines()[0].split("\t")[0]

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl NUMBA_CACHE_DIR=$PWD/.cache/numba
        mkdir -p {o}
        python {s}/upstream_layout.py labels --sheet {context.Input(study).container} \
            --table {context.Input(am).container} --skipped {o}/skipped_labels.tsv > labels.tsv
        while IFS=$'\\t' read -r L D L2; do
            mkdir -p {o}/$D
            python {s}/upstream_layout.py recolor --label "$L" {context.Input(am).container} am.tsv
            python {s}/bubbleplotter.py --input am.tsv --output-prefix {o}/$D/bubble_plot_asv \
                --count-col count --sample-col "{sid_col}" --group1-col "$L" --color-col Color \
                --group2-col "$L2" --no-auto-size --formats pdf,png,svg --figsize 32,60 \
                --bubble-scale 10
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
    group_by=study,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
