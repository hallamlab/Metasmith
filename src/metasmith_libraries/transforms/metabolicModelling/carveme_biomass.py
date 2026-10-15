from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
# `kbase/build_model/` has TWO producers of `modelling::metabolic_model` --
# `gem_from_gpr` (a draft) and `fetch_bigg_model` (a curated baseline) -- and this
# requirement cannot tell them apart on its own. Tony's approved design is
# GPR-table-first, so a TEMPLATE naming this transform must pin its target to
# `gem_from_gpr`'s output explicitly rather than leave this requirement to
# whichever producer the planner reaches first.
draft   = model.AddRequirement(lib.GetType("modelling::metabolic_model"))
universe = model.AddRequirement(lib.GetType("modelling::carveme_universe"))
xref    = model.AddRequirement(lib.GetType("ref::mnx_chem_xref"))
chem    = model.AddRequirement(lib.GetType("ref::mnx_chem_prop"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::biomass_model"))

def protocol(context: ExecutionContext):
    idraft = context.Input(draft)
    iuniv  = context.Input(universe)
    ixref  = context.Input(xref)
    ichem  = context.Input(chem)
    ihelp  = context.Input(helpers)
    iout   = context.Output(out)

    # `gem_from_gpr.py` sets no objective, deliberately -- a GPR table names no
    # biomass equation. Tony's approved design borrows CarveMe's `Growth` instead
    # of deriving one; this maps its 57 (or 64, for the gramneg universe) BiGG
    # metabolites into MNXM through the same three-tier bridge `bridge.py` uses,
    # and prints the tier yield every run -- see `carveme_biomass.py`'s own
    # docstring in `resources/lib/modelling/` for what each tier means and the
    # measured 57/57-direct result over the bacteria universe.
    _cmd = f"""\
            export XDG_CACHE_HOME=$TMPDIR
            python {ihelp.container}/carveme_biomass.py \
                {idraft.container} {iuniv.container} {ixref.container} {ichem.container} \
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
