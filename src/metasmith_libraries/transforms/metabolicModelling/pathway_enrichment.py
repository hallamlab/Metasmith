from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image    = model.AddRequirement(lib.GetType("env::metabolomics-python.env"))
diff_dir = model.AddRequirement(lib.GetType("metabolomics::metabolomics_differential"))
helpers = model.AddRequirement(lib.GetType("lib::metabolomics"))
out_dir  = model.AddProduct(lib.GetType("metabolomics::metabolomics_pathway_enrichment"))


def protocol(context: ExecutionContext):
    idiff = context.Input(diff_dir)
    ihelp    = context.Input(helpers)
    iout  = context.Output(out_dir)

    context.ExecWithEnv(
        env=image,
        binds=[(idiff.external, "/diff_data")],
        cmd=f"python {ihelp.container}/pathway_enrichment.py /diff_data {iout.container}",
    )

    return ExecutionResult(
        manifest=[{out_dir: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=diff_dir,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
