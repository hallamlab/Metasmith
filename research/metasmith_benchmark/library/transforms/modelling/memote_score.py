"""The standard memote_score, with $HOME on the task directory.

cobrapy creates its cache directory under $HOME on first import, and the grid's container
leaves $HOME pointing at an unbound, read-only /home, so the standard transform fails with
Errno 30 before scoring anything.
"""
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::memote.env"))
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

    context.ExecWithEnv(env=image, cmd=f"""
        export HOME="$PWD"
        python {ihelp.container}/memote_score.py \
            {idraft.container} {ireport.container} {iresults.container} {iscore.container}
    """)

    return ExecutionResult(
        manifest=[{report: ireport.local, results: iresults.local, score: iscore.local}],
        success=all(o.exists() for o in (ireport.local, iresults.local, iscore.local)),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=draft,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(minutes=30)),
)
