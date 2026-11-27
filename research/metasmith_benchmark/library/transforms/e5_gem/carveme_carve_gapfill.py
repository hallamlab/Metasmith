from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::carveme_166.env"))
hits    = model.AddRequirement(lib.GetType("bench::bigg_diamond_hits"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::carveme_model"))

# The helper's main() reading the previous step's hits instead of running its own search. It hardcodes
# input_type="protein", and editing it would move lib::modelling for every experiment that stages it,
# so the input type is overridden here, at the one call site.
RUN = """
import sys
sys.path.insert(0, "{helpers}")
import carveme.cli.carve as carve
import reframed.solvers
from carveme_from_orfs import carve_and_gapfill, read_medium_name

medium_name = read_medium_name("{medium}")
reframed.solvers.set_default_solver("scip")
maincall = carve.maincall
carve.maincall = lambda **kw: maincall(**{{**kw, "input_type": "diamond"}})
carve_and_gapfill("{hits}", "{media}", medium_name, "{out}")
"""


def protocol(context: ExecutionContext):
    ihits   = context.Input(hits)
    imedia  = context.Input(media)
    imedium = context.Input(medium)
    ihelp   = context.Input(helpers)
    iout    = context.Output(out)

    script = RUN.format(helpers=ihelp.container, hits=ihits.container, media=imedia.container,
                        medium=imedium.container, out=iout.container)
    context.ExecWithEnv(env=image, cmd=f"""
        export XDG_CACHE_HOME=$TMPDIR
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
    resources=Resources(cpus=1, memory=Size.GB(16), duration=Duration(hours=2)),
)
