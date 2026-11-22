from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm   = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
calls = [
    model.AddRequirement(lib.GetType("annotation::antismash_json"), parents={asm}),
]
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
gpr_lib = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
out     = model.AddProduct(lib.GetType("annotation::antismash_lane_table"))

THREADS = 1


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iout  = context.Output(out)
    paths = " ".join(str(context.Input(c).container) for c in calls)
    context.ExecWithEnv(env=image, cmd=f"""
        python3 {context.Input(gpr_lib).container}/annotation_table.py \
            --ev-lib {context.Input(ev_lib).container} \
            --orfs {iorfs.container} --source {iorfs.local.stem} \
            --lane antismash {paths} --out {iout.container}
    """)
    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=THREADS, memory=Size.GB(8), duration=Duration(hours=1)),
)
