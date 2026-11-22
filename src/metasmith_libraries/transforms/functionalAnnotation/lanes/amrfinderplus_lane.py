from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
norm  = model.AddRequirement(lib.GetType("env::argnorm.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
calls = [
    model.AddRequirement(lib.GetType("annotation::amrfinderplus_results"), parents={orfs}),
]
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
gpr_lib = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
out     = model.AddProduct(lib.GetType("annotation::amrfinderplus_lane_table"))

THREADS = 1


# argNorm names the lane's genes as AROs, as PathoFact 2.0's rule argnorm_amrfinder does;
# the lane table carries them as extra columns.
def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iout  = context.Output(out)
    context.ExecWithEnv(env=norm, cmd=f"argnorm amrfinderplus -i {context.Input(calls[0]).container} -o normalised.tsv")
    paths = "normalised.tsv"
    context.ExecWithEnv(env=image, cmd=f"""
        python3 {context.Input(gpr_lib).container}/annotation_table.py \
            --ev-lib {context.Input(ev_lib).container} \
            --orfs {iorfs.container} --source {iorfs.local.stem} \
            --lane amrfinderplus {paths} --out {iout.container}
    """)
    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=THREADS, memory=Size.GB(8), duration=Duration(hours=1)),
)
