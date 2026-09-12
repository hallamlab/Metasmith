from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image   = model.AddRequirement(lib.GetType("env::metabolomics-python.env"))
dataset = model.AddRequirement(lib.GetType("metabolomics::jgi_metabolomics_dataset"))
helpers = model.AddRequirement(lib.GetType("lib::metabolomics"))
out_ft  = model.AddProduct(lib.GetType("metabolomics::metabolomics_feature_table"))


def protocol(context: ExecutionContext):
    idataset = context.Input(dataset)
    ihelp    = context.Input(helpers)
    iout     = context.Output(out_ft)

    context.ExecWithEnv(
        env=image,
        binds=[(idataset.external, "/jgi_data")],
        cmd=f"python {ihelp.container}/jgi_loader.py /jgi_data {iout.container}",
    )

    return ExecutionResult(
        manifest=[{out_ft: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dataset,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
