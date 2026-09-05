from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
gem     = model.AddRequirement(lib.GetType("modelling::metabolic_model"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
conds   = model.AddRequirement(lib.GetType("ecspr::conditions"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::fba_solution"))

def protocol(context: ExecutionContext):
    igem=context.Input(gem)
    imedia=context.Input(media)
    iconds=context.Input(conds)
    ihelp=context.Input(helpers)
    iout=context.Output(out)

    # A conditions table is one row per medium and mask, which is a KBase
    # PhenotypeSet in this repository's own idiom -- so `predict_phenotype` is
    # absorbed here rather than becoming a transform of its own.
    #
    # Round 3 declared `ecspr::conditions` OPTIONAL and metasmith has no optional
    # requirement, so round 4 made it mandatory and left the trade to whoever wrote
    # this body. It stays one transform: the alternative was two transforms
    # differing only in whether a table is read, and a template supplying a
    # one-row table costs less than that. What the body does NOT do is invent a
    # meaning for the ecspr mask columns -- `media` and `drop_*` carry over to a
    # cobra model, `background_*` and `mask_*` say how a model is built rather than
    # how a built one is solved, and lib::modelling/cobra_fba.py names the ones it
    # ignored in its own output.
    _cmd = f"""\
            # cobra builds a Configuration at IMPORT and that constructor makes its
            # cache directory. The container runs as the calling uid with no passwd
            # entry, so $HOME is / and the mkdir fails before a single line of the
            # entry point has run. XDG_CACHE_HOME is what platformdirs reads first.
            export XDG_CACHE_HOME=$TMPDIR
            python {ihelp.container}/cobra_fba.py \
                {igem.container} {imedia.container} {iconds.container} {iout.container}
        """
    context.ExecWithEnv().ifContainerDo(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=gem,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
