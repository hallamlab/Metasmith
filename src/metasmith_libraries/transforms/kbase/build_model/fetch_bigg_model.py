from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
acc     = model.AddRequirement(lib.GetType("modelling::bigg_model_id"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::metabolic_model"))

def protocol(context: ExecutionContext):
    iacc=context.Input(acc)
    ihelp=context.Input(helpers)
    iout=context.Output(out)

    # The id is the accession, the way `ncbi::assembly_accession` is, so this
    # matches `logistics/getNcbiAssembly.py` in shape. A curated biomass equation
    # and curated GPR rules are the sound baseline a draft reconstruction is
    # measured against, which is why the curated model is fetched rather than
    # rebuilt.
    _cmd = f"""\
            python {ihelp.container}/fetch_bigg_model.py {iacc.container} {iout.container}
        """
    context.ExecWithEnv().ifContainerDo(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=acc,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(minutes=30),
    ),
)
