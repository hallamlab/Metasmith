from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e4abl::carveme_166.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("e4abl::model_scip"))


def protocol(context: ExecutionContext):
    iorfs   = context.Input(orfs)
    imedia  = context.Input(media)
    imedium = context.Input(medium)
    ihelp   = context.Input(helpers)
    iout    = context.Output(out)

    # The modern lane's bench carveme_from_orfs, on this image: no CPLEX, so SCIP.
    context.ExecWithEnv(env=image, cmd=f"""
        export XDG_CACHE_HOME=$TMPDIR
        python -c 'from reframed.solvers import get_default_solver as s; print("solver", s()); assert s() == "scip"'
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
