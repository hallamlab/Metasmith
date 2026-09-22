"""The standard carveme_from_orfs on CarveMe 1.6.6, whose solves stop at 600 s.

On 1.6.1 SCIP stalled in gapfill on 93% of E5 pratama's bins and 21% of metaGEM's.
"""
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::carveme_166.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::carveme_model"))


def protocol(context: ExecutionContext):
    iorfs   = context.Input(orfs)
    imedia  = context.Input(media)
    imedium = context.Input(medium)
    ihelp   = context.Input(helpers)
    iout    = context.Output(out)

    context.ExecWithEnv(env=image, cmd=f"""
        export XDG_CACHE_HOME=$TMPDIR
        python {ihelp.container}/carveme_from_orfs.py \
            {iorfs.container} {imedia.container} {imedium.container} {iout.container}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=2)),
)
