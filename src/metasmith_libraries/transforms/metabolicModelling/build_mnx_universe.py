from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
draft   = model.AddRequirement(lib.GetType("modelling::transport_model"))
reac    = model.AddRequirement(lib.GetType("ref::mnx_reac_prop"))
chem    = model.AddRequirement(lib.GetType("ref::mnx_chem_prop"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::mnx_gapfill_universe"))

def protocol(context: ExecutionContext):
    idraft = context.Input(draft)
    ireac  = context.Input(reac)
    ichem  = context.Input(chem)
    ihelp  = context.Input(helpers)
    iout   = context.Output(out)

    # Deliberately UNRESTRICTED -- the opposite of `cobra_gapfill.py`'s universe --
    # so that `carveme_gapfill.py` exercises CarveMe's gapfill as it actually
    # behaves, free to reach for any single-compartment reaction in this MetaNetX
    # release. Runs under `env::cobra.env` rather than `env::carveme.env` because
    # the CarveMe biocontainer ships `reframed`, not `cobra`; the gapfill step
    # itself is a separate transform for exactly that reason.
    _cmd = f"""\
            export XDG_CACHE_HOME=$TMPDIR
            python {ihelp.container}/build_mnx_universe.py \
                {idraft.container} {ireac.container} {ichem.container} {iout.container}
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
        memory=Size.GB(16),
        duration=Duration(hours=1),
    ),
)
