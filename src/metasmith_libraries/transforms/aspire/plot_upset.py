# plot_upset -- PLOT_UPSET (asv_pipeline.nf:3927), once per label of the sample sheet with at
# least two levels, at upstream's config defaults (micro domain, no Venn diagrams).
#
# Upstream re-opens the target and final count tables and the taxonomy from its output tree;
# here each path is passed explicitly, and the plots land in <label>/metadata/. Its second,
# `raw`-tagged pass reads the tables from before control subtraction, which the port does not
# do, so that pass would only repeat this one.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
md      = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={study})
clean   = model.AddRequirement(lib.GetType("aspire::counts_clean"), parents={study})
counts  = model.AddRequirement(lib.GetType("aspire::analysis_counts"), parents={study})
tax     = model.AddRequirement(lib.GetType("amplicon::asv_taxonomy"), parents={study})
out     = model.AddProduct(lib.GetType("aspire::upset_plots"))


def protocol(context: ExecutionContext):
    iout, s = context.Output(out), context.Input(scripts).container
    o = iout.container
    sid_col = context.Input(study).local.read_text().splitlines()[0].split("\t")[0]

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl NUMBA_CACHE_DIR=$PWD/.cache/numba
        mkdir -p {o}
        python {s}/upstream_layout.py labels --sheet {context.Input(study).container} \
            --table {context.Input(md).container} --skipped {o}/skipped_labels.tsv > labels.tsv
        while IFS=$'\\t' read -r L D L2; do
            mkdir -p {o}/$D
            python {s}/upstream_layout.py recolor --label "$L" {context.Input(md).container} md.tsv
            python {s}/plot_upset.py --data-dir {o}/$D --subdir . --domain micro \
                --taxonomy-path {context.Input(tax).container} --metadata-path md.tsv \
                --asv-raw-path {context.Input(clean).container} \
                --asv-final-path {context.Input(counts).container} \
                --sample-id-col "{sid_col}" --group-col "$L" --color-col Color \
                --skip-venn --formats pdf,svg,png --font-size 12
        done < labels.tsv
        rm -f labels.tsv md.tsv
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
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
