from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::memote.env"))
# The SUPERTYPE, not `gapfilled_model` or `carveme_model` by name -- MEMOTE scores
# any producer of `metabolic_model`, which is the whole reason this is one
# transform rather than two. See `data_types/modelling.yml`'s `memote_report`
# comment for why that is safe even though `metabolic_model` itself has several
# producers.
draft   = model.AddRequirement(lib.GetType("modelling::metabolic_model"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
report  = model.AddProduct(lib.GetType("modelling::memote_report"))
results = model.AddProduct(lib.GetType("modelling::memote_results"))
score   = model.AddProduct(lib.GetType("modelling::memote_score"))

def protocol(context: ExecutionContext):
    idraft   = context.Input(draft)
    ihelp    = context.Input(helpers)
    ireport  = context.Output(report)
    iresults = context.Output(results)
    iscore   = context.Output(score)

    _cmd = f"""\
            python {ihelp.container}/memote_score.py \
                {idraft.container} {ireport.container} {iresults.container} {iscore.container}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    manifest = [{report: ireport.local, results: iresults.local, score: iscore.local}]
    return ExecutionResult(
        manifest=manifest,
        success=all(o.exists() for o in (ireport.local, iresults.local, iscore.local)),
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
