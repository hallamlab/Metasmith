from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
draft   = model.AddRequirement(lib.GetType("modelling::metabolic_model"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
bridge  = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
# The universe a gapfill draws from is a set of equations, which mnxr_lookup does
# not carry; chem_xref is what maps a MetaNetX metabolite onto the dialect of a
# model that came from BiGG rather than from `gem_from_gpr`.
reac    = model.AddRequirement(lib.GetType("ref::mnx_reac_prop"))
chem    = model.AddRequirement(lib.GetType("ref::mnx_chem_prop"))
xref    = model.AddRequirement(lib.GetType("ref::mnx_chem_xref"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::gapfilled_model"))

def protocol(context: ExecutionContext):
    idraft=context.Input(draft)
    imedia=context.Input(media)
    ireac=context.Input(reac)
    ichem=context.Input(chem)
    ixref=context.Input(xref)
    ihelp=context.Input(helpers)
    iout=context.Output(out)

    # Without this a missing annotation returns zero growth, which reads as a
    # biological result. Every reaction added is labelled, so a later reader can
    # tell a gapfill from an annotation.
    _cmd = f"""\
            # cobra builds a Configuration at IMPORT and that constructor makes its
            # cache directory. The container runs as the calling uid with no passwd
            # entry, so $HOME is / and the mkdir fails before a single line of the
            # entry point has run. XDG_CACHE_HOME is what platformdirs reads first.
            export XDG_CACHE_HOME=$TMPDIR
            python {ihelp.container}/cobra_gapfill.py \
                {idraft.container} {imedia.container} \
                {ireac.container} {ichem.container} {ixref.container} \
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
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
