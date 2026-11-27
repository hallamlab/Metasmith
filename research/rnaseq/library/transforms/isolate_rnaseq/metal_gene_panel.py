from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
scripts = model.AddRequirement(lib.GetType("lib::isolate_rnaseq"))
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
trend   = model.AddRequirement(lib.GetType("isolate_rnaseq::dose_trend_results"), parents={exp})
bacmet  = model.AddRequirement(lib.GetType("annotation::bacmet_diamond_results"), parents={exp})
gff     = model.AddRequirement(lib.GetType("sequences::gff"), parents={exp})
out     = model.AddProduct(lib.GetType("isolate_rnaseq::metal_gene_panel"))


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    context.ExecWithEnv(env=image, cmd=f"""\
        export HOME=$PWD MPLCONFIGDIR=$PWD/.mpl
        python {context.Input(scripts).container}/metal_panel.py {context.Input(trend).container} \\
            {context.Input(bacmet).container} {context.Input(gff).container} {iout.container}
    """)
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / "metal_gene_panel.csv").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(minutes=30)),
)
