from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e4abl::carveme_166.env"))
hits    = model.AddRequirement(lib.GetType("e4abl::bigg166_diamond_hits"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
cplex   = model.AddRequirement(lib.GetType("modelling::cplex_installation"))
out     = model.AddProduct(lib.GetType("e4abl::model_gapfill"))

# The modern lane's two-step carve_and_gapfill, reading the DIAMOND 0.9.30 hits instead of running
# its own search. The helper hardcodes input_type="protein", and editing it would move lib::modelling
# for every experiment that stages it, so the input type is overridden here, at the one call site.
RUN = """
import sys
sys.path.insert(0, "{helpers}")
import carveme.cli.carve as carve
from reframed.solvers import get_default_solver
from carveme_from_orfs import carve_and_gapfill, read_medium_name

print("solver", get_default_solver())
assert get_default_solver() == "cplex"
maincall = carve.maincall
carve.maincall = lambda **kw: maincall(**{{**kw, "input_type": "diamond"}})
carve_and_gapfill("{hits}", "{media}", read_medium_name("{medium}"), "{out}")
"""


def protocol(context: ExecutionContext):
    ihits   = context.Input(hits)
    imedia  = context.Input(media)
    imedium = context.Input(medium)
    ihelp   = context.Input(helpers)
    icplex  = context.Input(cplex)
    iout    = context.Output(out)

    script = RUN.format(helpers=ihelp.container, hits=ihits.container, media=imedia.container,
                        medium=imedium.container, out=iout.container)
    context.ExecWithEnv(env=image, cmd=f"""
        export XDG_CACHE_HOME=$TMPDIR
        export PYTHONPATH={icplex.container}:$PYTHONPATH
        python - <<'PY'
{script}
PY
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
