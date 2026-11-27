from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::pydeseq2.env"))
scripts = model.AddRequirement(lib.GetType("lib::isolate_rnaseq"))
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
counts  = model.AddRequirement(lib.GetType("transcriptomics::gene_count_table"), parents={exp})
raw     = model.AddRequirement(lib.GetType("isolate_rnaseq::raw_count_table"), parents={exp})
sheet   = model.AddRequirement(lib.GetType("isolate_rnaseq::sample_sheet"), parents={exp})
qc      = model.AddRequirement(lib.GetType("isolate_rnaseq::count_qc"), parents={exp})
trend   = model.AddProduct(lib.GetType("isolate_rnaseq::dose_trend_results"))
figures = model.AddProduct(lib.GetType("isolate_rnaseq::dose_trend_figures"))


def protocol(context: ExecutionContext):
    itrend, ifig = context.Output(trend), context.Output(figures)
    context.ExecWithEnv(env=image, cmd=f"""\
        export HOME=$PWD MPLCONFIGDIR=$PWD/.mpl
        python {context.Input(scripts).container}/dose_trend.py {context.Input(counts).container} \\
            {context.Input(raw).container} {context.Input(sheet).container} {context.Input(qc).container} \\
            {itrend.container} {ifig.container}
    """)
    return ExecutionResult(
        manifest=[{trend: itrend.local, figures: ifig.local}],
        success=itrend.local.exists() and (ifig.local / "summary.txt").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(cpus=4, memory=Size.GB(16), duration=Duration(hours=1)),
)
