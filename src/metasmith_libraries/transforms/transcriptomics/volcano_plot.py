from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
exp    = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
de     = model.AddRequirement(lib.GetType("transcriptomics::deseq2_results"), parents={exp})
image  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
helpers = model.AddRequirement(lib.GetType("lib::transcriptomics"))
out    = model.AddProduct(lib.GetType("transcriptomics::volcano_plot"))
outpng = model.AddProduct(lib.GetType("transcriptomics::volcano_plot_png"))

def protocol(context: ExecutionContext):
    ide    = context.Input(de)
    ihelp = context.Input(helpers)
    iout   = context.Output(out)
    ioutpng = context.Output(outpng)

    context.ExecWithEnv(
        env=image,
        cmd=f"python {ihelp.container}/volcano_plot.py {ide.container} {iout.container} {ioutpng.container}",
    )
    return ExecutionResult(
        manifest=[{out: iout.local}, {outpng: ioutpng.local}],
        success=iout.local.exists() and ioutpng.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
