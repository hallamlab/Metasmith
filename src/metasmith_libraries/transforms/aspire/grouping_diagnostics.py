# grouping_diagnostics -- GROUPING_DIAGNOSTICS (asv_pipeline.nf:4929) over every label of the
# sample sheet at once, which the script loops over itself, skipping a label with too few
# groups. Upstream's config defaults, except that soft labelling is on: the three soft-label
# tables are this row's products. Nothing applies them, as GROUP_LABEL_AUGMENTATION, their only
# consumer in the .nf, is not ported. The power analysis stays off, as upstream's default.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
md      = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={study})
counts  = model.AddRequirement(lib.GetType("aspire::analysis_counts"), parents={study})
out     = model.AddProduct(lib.GetType("aspire::grouping_diagnostics_outputs"))
assign  = model.AddProduct(lib.GetType("aspire::soft_assignments"))
valid   = model.AddProduct(lib.GetType("aspire::soft_validation"))
vsum    = model.AddProduct(lib.GetType("aspire::soft_validation_summary"))


def protocol(context: ExecutionContext):
    outs = {p: context.Output(p) for p in (out, assign, valid, vsum)}
    s, o = context.Input(scripts).container, outs[out].container
    header = context.Input(study).local.read_text().splitlines()[0].split("\t")
    sid_col, labels = header[0], header[1:]
    t = f"{o}/tables/grouping_soft_label"

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl NUMBA_CACHE_DIR=$PWD/.cache/numba
        python {s}/grouping_diagnostics.py --metadata {context.Input(md).container} \
            --asv-counts {context.Input(counts).container} --outdir {o} \
            --sample-col "{sid_col}" --group-cols "{','.join(labels)}" \
            --metrics bray --transform relative --permutations 999 --random-state 42 \
            --formats pdf,png,svg --soft-label-missing --soft-label-k 7 \
            --soft-label-exclude-labels outlier --soft-label-min-class-samples 3 \
            --soft-label-distance-quantile 0.95 --power-min-groups 2
        cp {t}_assignments.tsv {outs[assign].container}
        cp {t}_validation.tsv {outs[valid].container}
        cp {t}_validation_summary.tsv {outs[vsum].container}
        rm -rf .cache
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
