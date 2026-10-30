from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
scripts = model.AddRequirement(lib.GetType("lib::isolate_rnaseq"))
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
trend   = model.AddRequirement(lib.GetType("isolate_rnaseq::dose_trend_results"), parents={exp})
sheet   = model.AddRequirement(lib.GetType("isolate_rnaseq::sample_sheet"), parents={exp})
out     = model.AddProduct(lib.GetType("isolate_rnaseq::dose_clusters"))


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    top = max(context.Input(sheet).local.read_text().splitlines()[1:], key=lambda l: float(l.split("\t")[1]))
    top_label = f"{float(top.split(chr(9))[1]):g}ppm"
    run = f"python {context.Input(scripts).container}/cluster_k.py {context.Input(trend).container} {iout.container}"
    # The highest dose is the library most likely to drive a shape on its own, so the choice of
    # k is repeated without it.
    context.ExecWithEnv(env=image, cmd=f"""\
        export HOME=$PWD MPLCONFIGDIR=$PWD/.mpl OMP_NUM_THREADS={context.params.get("cpus") or 4}
        {run}
        {run} --drop-dose {top_label}
    """)
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / f"k_selection_summary_without_{top_label}.txt").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(cpus=4, memory=Size.GB(8), duration=Duration(hours=4)),
)
