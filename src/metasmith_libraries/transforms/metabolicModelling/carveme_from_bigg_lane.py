from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::carveme.env"))
python  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
# CarveMe annotates nothing itself: the bigg lane is its DIAMOND search, run once on the
# assembly's ORFs, and each MAG takes the hits on its own contigs.
bigg    = model.AddRequirement(lib.GetType("annotation::bigg_lane_table"), parents={orfs})
mag     = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"), parents={asm})
media   = model.AddRequirement(lib.GetType("modelling::media"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
out     = model.AddProduct(lib.GetType("modelling::carveme_model"))


def protocol(context: ExecutionContext):
    ihelp = context.Input(helpers)
    iout  = context.Output(out)
    context.ExecWithEnv(env=python, cmd=f"""
        python3 {ihelp.container}/bigg_lane_hits.py --ev-lib {context.Input(ev_lib).container} \
            --lane-table {context.Input(bigg).container} --mag {context.Input(mag).container} \
            --out bigg_hits.tsv
    """)
    context.ExecWithEnv(env=image, cmd=f"""
        export XDG_CACHE_HOME=$TMPDIR
        python {ihelp.container}/carveme_from_orfs.py bigg_hits.tsv \
            {context.Input(media).container} {context.Input(medium).container} {iout.container} diamond
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=mag,
    resources=Resources(
        cpus=1,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)
