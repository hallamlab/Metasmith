from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e4::carveme_122.env"))
hits    = model.AddRequirement(lib.GetType("e4abl::bigg166_diamond_hits"))
gprs    = model.AddRequirement(lib.GetType("e4abl::carveme166_bigg_gprs"))
mediadb = model.AddRequirement(lib.GetType("bench::metagem_media_db"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
univ    = model.AddRequirement(lib.GetType("e4abl::carveme166_universe"))
cplex   = model.AddRequirement(lib.GetType("modelling::cplex_installation"))
out     = model.AddProduct(lib.GetType("e4abl::model_universe"))

# CarveMe 1.2.2 reads its gene-reaction table from its own package directory (project_dir, from
# carveme/__init__.py's __file__), so the swap is a package of symlinks into the image's copy with
# 1.6.6's table in place of 1.2.2's, first on PYTHONPATH. 1.2.2's table has 306,100 rows.
SWAP_GPRS = """
    PD=$(python -c 'import importlib.util, os; print(os.path.dirname(importlib.util.find_spec("carveme").origin))')
    link_except() {{ mkdir -p "$2"; for e in "$1"/*; do [ "$(basename "$e")" = "$3" ] || ln -s "$e" "$2/"; done; }}
    link_except "$PD" pkg/carveme data
    link_except "$PD/data" pkg/carveme/data generated
    link_except "$PD/data/generated" pkg/carveme/data/generated bigg_gprs.csv.gz
    cp {gprs} pkg/carveme/data/generated/bigg_gprs.csv.gz
    export PYTHONPATH=$PWD/pkg:{cplex}:$PYTHONPATH
    python -c 'import pandas as pd; from carveme import config, project_dir; p = project_dir + config.get("generated", "bigg_gprs"); n = len(pd.read_csv(p)); print(f"bigg_gprs {{p}}: {{n}} rows"); assert n == 108091, n'
"""


def protocol(context: ExecutionContext):
    ihits    = context.Input(hits)
    igprs    = context.Input(gprs)
    imediadb = context.Input(mediadb)
    imedium  = context.Input(medium)
    iuniv    = context.Input(univ)
    icplex   = context.Input(cplex)
    iout     = context.Output(out)

    # carve_bigg, carving from 1.6.6's universe.
    context.ExecWithEnv(env=image, cmd=SWAP_GPRS.format(gprs=igprs.container, cplex=icplex.container) + f"""
        carve --diamond {ihits.container} \
            -g $(cat {imedium.container}) \
            -v \
            --mediadb {imediadb.container} \
            --fbc2 \
            --universe-file {iuniv.container} \
            -o {iout.container}
        rm -rf pkg
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
