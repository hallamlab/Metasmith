from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e4abl::carveme_166.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
cplex   = model.AddRequirement(lib.GetType("modelling::cplex_installation"))
out     = model.AddProduct(lib.GetType("e4abl::model_diamond"))


def protocol(context: ExecutionContext):
    iorfs   = context.Input(orfs)
    imedia  = context.Input(media)
    imedium = context.Input(medium)
    ihelp   = context.Input(helpers)
    icplex  = context.Input(cplex)
    iout    = context.Output(out)

    # The standard CPLEX variant, unchanged: CarveMe runs its own DIAMOND 2.1.13 search.
    context.ExecWithEnv(env=image, cmd=f"""
        export XDG_CACHE_HOME=$TMPDIR
        export PYTHONPATH={icplex.container}:$PYTHONPATH
        python -c 'from reframed.solvers import get_default_solver as s; print("solver", s()); assert s() == "cplex"'
        python {ihelp.container}/carveme_from_orfs_cplex.py \
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
    resources=Resources(
        cpus=4,
        memory=Size.GB(32).SetStrict(),
        duration=Duration(hours=6).SetStrict(),
    ),
)
