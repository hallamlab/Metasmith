from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image    = model.AddRequirement(lib.GetType("env::metabolomics-python.env"))
diff_dir = model.AddRequirement(lib.GetType("metabolomics::metabolomics_differential"))
sbml     = model.AddRequirement(lib.GetType("metabolomics::metabolic_model_sbml"))
helpers = model.AddRequirement(lib.GetType("lib::metabolomics"))
out_dir  = model.AddProduct(lib.GetType("metabolomics::metabolomics_fba_results"))


def protocol(context: ExecutionContext):
    idiff  = context.Input(diff_dir)
    ihelp    = context.Input(helpers)
    isbml  = context.Input(sbml)
    iout   = context.Output(out_dir)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (idiff.external, "/diff_data"),
            (isbml.external.parent, "/model_dir"),
        ],
        cmd=f"python {ihelp.container}/fba_constraint.py /diff_data /model_dir/{isbml.external.name} {iout.container}",
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
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
