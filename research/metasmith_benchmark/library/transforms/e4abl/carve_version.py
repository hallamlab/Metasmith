from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e4abl::carveme_166.env"))
hits    = model.AddRequirement(lib.GetType("e4abl::bigg166_diamond_hits"))
mediadb = model.AddRequirement(lib.GetType("bench::metagem_media_db"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
cplex   = model.AddRequirement(lib.GetType("modelling::cplex_installation"))
out     = model.AddProduct(lib.GetType("e4abl::model_version"))


def protocol(context: ExecutionContext):
    ihits    = context.Input(hits)
    imediadb = context.Input(mediadb)
    imedium  = context.Input(medium)
    icplex   = context.Input(cplex)
    iout     = context.Output(out)

    # metaGEM's carve call on CarveMe 1.6.6, which carves from its own bundled gene reference and
    # universe. reframed takes CPLEX because it imports.
    context.ExecWithEnv(env=image, cmd=f"""
        export XDG_CACHE_HOME=$TMPDIR
        export PYTHONPATH={icplex.container}:$PYTHONPATH
        python -c 'from reframed.solvers import get_default_solver as s; print("solver", s()); assert s() == "cplex"'
        carve --diamond {ihits.container} \
            -g $(cat {imedium.container}) \
            -v \
            --mediadb {imediadb.container} \
            --fbc2 \
            -o {iout.container}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=hits,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32).SetStrict(),
        duration=Duration(hours=6).SetStrict(),
    ),
)
