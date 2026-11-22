from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
calls = [
    model.AddRequirement(lib.GetType("annotation::pathofact_tox_hmm"), parents={orfs}),
    model.AddRequirement(lib.GetType("annotation::pathofact_vf_hmm"), parents={orfs}),
    model.AddRequirement(lib.GetType("annotation::pathofact_hmm_descriptions"), parents={orfs}),
]
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
gpr_lib = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
out     = model.AddProduct(lib.GetType("annotation::pathofact_hmm_lane_table"))

THREADS = 1


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iout  = context.Output(out)
    paths = " ".join(str(context.Input(c).container) for c in calls)
    context.ExecWithEnv(env=image, cmd=f"""
        python3 {context.Input(gpr_lib).container}/annotation_table.py \
            --ev-lib {context.Input(ev_lib).container} \
            --orfs {iorfs.container} --source {iorfs.local.stem} \
            --lane pathofact_hmm {paths} --out {iout.container}
    """)
    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=THREADS, memory=Size.GB(8), duration=Duration(hours=1)),
)
