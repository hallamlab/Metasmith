from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
draft   = model.AddRequirement(lib.GetType("modelling::biomass_model"))
reac    = model.AddRequirement(lib.GetType("ref::mnx_reac_prop"))
chem    = model.AddRequirement(lib.GetType("ref::mnx_chem_prop"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::transport_model"))

def protocol(context: ExecutionContext):
    idraft = context.Input(draft)
    ireac  = context.Input(reac)
    ichem  = context.Input(chem)
    imedia = context.Input(media)
    ihelp  = context.Input(helpers)
    iout   = context.Output(out)

    # Transport is taken NATIVELY from MetaNetX's own `reac_prop.is_transport`, not
    # borrowed from CarveMe's 1,870 BiGG-namespaced transporters -- see
    # `carveme_transport.py`'s own docstring in `resources/lib/modelling/` for why,
    # and for why the transporter+exchange set added here is kept to exactly what
    # this medium and this draft's own biomass precursors need.
    _cmd = f"""\
            export XDG_CACHE_HOME=$TMPDIR
            python {ihelp.container}/carveme_transport.py \
                {idraft.container} {ireac.container} {ichem.container} {imedia.container} \
                {iout.container}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=draft,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(minutes=30),
    ),
)
