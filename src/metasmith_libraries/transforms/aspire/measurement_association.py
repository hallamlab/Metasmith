# measurement_association -- MEASUREMENT_ASSOCIATION (asv_pipeline.nf:4875) at upstream's
# config defaults, grouped by the sheet's first label as upstream groups by its type column.
# The measurements join on the sheet's sample column, and every numeric column is a
# measurement.
#
# This row reads no params, so a count table from outside ASPIRE can reach it.

from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts  = model.AddRequirement(lib.GetType("lib::aspire"))
survey   = model.AddRequirement(lib.GetType("amplicon::survey"))
counts   = model.AddRequirement(lib.GetType("amplicon::abundance_table"), parents={survey})
am       = model.AddRequirement(lib.GetType("aspire::analysis_asv_meta"), parents={survey})
md       = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={survey})
measures = model.AddRequirement(lib.GetType("aspire::sample_measurements"), parents={survey})
out      = model.AddProduct(lib.GetType("aspire::measurement_association_outputs"))


def protocol(context: ExecutionContext):
    iout, s = context.Output(out), context.Input(scripts).container
    header = context.Input(survey).local.read_text().splitlines()[0].split("\t")
    sid_col, label = header[0], header[1]

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl NUMBA_CACHE_DIR=$PWD/.cache/numba
        mkdir -p {iout.container}
        python {s}/measurement_association.py --asv-meta {context.Input(am).container} \
            --metadata {context.Input(md).container} --asv-counts {context.Input(counts).container} \
            --measurement-table {context.Input(measures).container} --outdir {iout.container} \
            --r-script {s}/run_measurement_association.R --sample-col "{sid_col}" \
            --asv-id-col ASV_ID --measurement-sample-col "{sid_col}" \
            --metadata-join-cols "" --measurement-join-cols "" --group-col "{label}" \
            --group-palette "" --max-asvs 300 --min-total 0 --min-prevalence 0 \
            --top-correlations 100 --correlation-direction both \
            --ordination-methods cca,rda,dbrda --permutations 999 --top-vectors 12 \
            --formats pdf,png,svg
        rm -rf .cache
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and any(iout.local.iterdir()),
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
