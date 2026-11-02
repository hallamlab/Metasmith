from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::carveme.env"))
draft    = model.AddRequirement(lib.GetType("modelling::transport_model"))
universe = model.AddRequirement(lib.GetType("modelling::mnx_gapfill_universe"))
media    = model.AddRequirement(lib.GetType("modelling::media"))
helpers  = model.AddRequirement(lib.GetType("lib::modelling"))
out      = model.AddProduct(lib.GetType("modelling::gapfilled_model"))

def protocol(context: ExecutionContext):
    idraft = context.Input(draft)
    iuniv  = context.Input(universe)
    imedia = context.Input(media)
    ihelp  = context.Input(helpers)
    iout   = context.Output(out)

    # CAUTION `modelling::gapfilled_model` has TWO producers: this transform and
    # `kbase/gapfill_model/cobra_gapfill.py`. They answer different questions --
    # see that type's comment in `data_types/modelling.yml` -- so a template
    # wanting a specific one must pin its target rather than name
    # `gapfilled_model` and let the planner choose.
    #
    # `medium_name` is fixed to "M9" here because the only worked example today,
    # `research/metasmith_libraries/carveme_m9_medium.tsv`, carries a single medium
    # named that; a template supplying a multi-medium table must also change this
    # argument. `modelling::media` itself ships no instance -- it is a deferred
    # template input, same as every other medium table this repo reads.
    _cmd = f"""\
            python {ihelp.container}/carveme_gapfill.py \
                {idraft.container} {iuniv.container} {imedia.container} M9 {iout.container}
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
        duration=Duration(hours=2),
    ),
)
