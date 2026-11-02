from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image   = model.AddRequirement(lib.GetType("env::metabolomics-python.env"))
feat_ft = model.AddRequirement(lib.GetType("metabolomics::metabolomics_feature_table"))
helpers = model.AddRequirement(lib.GetType("lib::metabolomics"))
out_dir = model.AddProduct(lib.GetType("metabolomics::metabolomics_differential"))


def protocol(context: ExecutionContext):
    ift  = context.Input(feat_ft)
    ihelp    = context.Input(helpers)
    iout = context.Output(out_dir)

    context.ExecWithEnv(
        env=image,
        cmd=f"python {ihelp.container}/differential_analysis.py {ift.container} {iout.container}",
    )

    return ExecutionResult(
        manifest=[{out_dir: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=feat_ft,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
