from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e4::carveme_122.env"))
hits    = model.AddRequirement(lib.GetType("bench::bigg_diamond_hits"))
mediadb = model.AddRequirement(lib.GetType("bench::metagem_media_db"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
cplex   = model.AddRequirement(lib.GetType("modelling::cplex_installation"))
out     = model.AddProduct(lib.GetType("modelling::carveme_model_cplex"))


def protocol(context: ExecutionContext):
    ihits   = context.Input(hits)
    imediadb = context.Input(mediadb)
    imedium = context.Input(medium)
    icplex  = context.Input(cplex)
    iout    = context.Output(out)

    # metaGEM's `carve -g <medium> -v --mediadb media_db.tsv --fbc2`, reading the search from the
    # previous step instead of running it. framed 0.5.1 finds CPLEX on PYTHONPATH.
    context.ExecWithEnv(env=image, cmd=f"""
        export PYTHONPATH={icplex.container}:$PYTHONPATH
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
