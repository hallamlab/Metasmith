# collectors_curve -- COLLECTORS_CURVE (asv_pipeline.nf:4364), once per label of the sample
# sheet with at least two levels, at upstream's config defaults.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
counts  = model.AddRequirement(lib.GetType("aspire::analysis_counts"), parents={study})
md      = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={study})
out     = model.AddProduct(lib.GetType("aspire::collectors_outputs"))


def protocol(context: ExecutionContext):
    iout, s = context.Output(out), context.Input(scripts).container
    sid_col = context.Input(study).local.read_text().splitlines()[0].split("\t")[0]

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        mkdir -p {iout.container}
        python {s}/upstream_layout.py labels --sheet {context.Input(study).container} \
            --table {context.Input(md).container} --skipped {iout.container}/skipped_labels.tsv > labels.tsv
        while IFS=$'\\t' read -r L D L2; do
            mkdir -p {iout.container}/$D
            python {s}/upstream_layout.py recolor --label "$L" {context.Input(md).container} md.tsv
            python {s}/collectors_curve.py --counts {context.Input(counts).container} --meta md.tsv \
                --sample-col "{sid_col}" --group-col "$L" --color-col Color \
                --permutations 999 --seed 42 --out_prefix {iout.container}/$D/collectors_curve \
                --formats pdf,svg --xpad 0.5 --max-cols 3 --show-perms 10 --presence-threshold 0
        done < labels.tsv
        rm -f labels.tsv md.tsv
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
        duration=Duration(hours=2),
    ),
)
