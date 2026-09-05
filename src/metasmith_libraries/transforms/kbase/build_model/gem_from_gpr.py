from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
gpr     = model.AddRequirement(lib.GetType("annotation::gpr_table"))
bridge  = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
vocab   = model.AddRequirement(lib.GetType("ref::metabolism_vocab"))
# The lookup answers "which MNXR" and the vocabulary answers "which of those the
# bake knows"; neither carries a stoichiometry, and a reconstruction is exactly a
# set of stoichiometries. reac_prop is where they live and chem_prop is where the
# formulas of the metabolites they name live. Added in round 5, when the body was
# written and the gap became unignorable.
reac    = model.AddRequirement(lib.GetType("ref::mnx_reac_prop"))
chem    = model.AddRequirement(lib.GetType("ref::mnx_chem_prop"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::metabolic_model"))

def protocol(context: ExecutionContext):
    igpr=context.Input(gpr)
    ibridge=context.Input(bridge)
    ivocab=context.Input(vocab)
    ireac=context.Input(reac)
    ichem=context.Input(chem)
    ihelp=context.Input(helpers)
    iout=context.Output(out)

    # CAUTION `kbase/build_model/` has TWO producers of `modelling::metabolic_model`,
    # this one and `fetch_bigg_model.py`, deliberately -- a draft and its curated
    # baseline. A target downstream of the type is therefore ambiguous and must be
    # pinned to one.
    _cmd = f"""\
            # cobra builds a Configuration at IMPORT and that constructor makes its
            # cache directory. The container runs as the calling uid with no passwd
            # entry, so $HOME is / and the mkdir fails before a single line of the
            # entry point has run. XDG_CACHE_HOME is what platformdirs reads first.
            export XDG_CACHE_HOME=$TMPDIR
            python {ihelp.container}/gem_from_gpr.py \
                {igpr.container} {ibridge.container} {ivocab.container} \
                {ireac.container} {ichem.container} {iout.container}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=gpr,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)
