from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

python  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"))
hits    = model.AddRequirement(lib.GetType("annotation::diamond_uniref90_results_chunk"), parents={orfs})
descs   = model.AddRequirement(lib.GetType("annotation::diamond_uniref90_descriptions_chunk"), parents={orfs})
bsr_lib = model.AddRequirement(lib.GetType("lib::diamond_bsr.py"))
out     = model.AddProduct(lib.GetType("annotation::diamond_uniref90_results"))
out_d   = model.AddProduct(lib.GetType("annotation::diamond_uniref90_descriptions"))


def protocol(context: ExecutionContext):
    ihits = " ".join(str(p.container) for p in sorted(context.InputGroup(hits), key=lambda p: str(p.local)))
    idescs = " ".join(str(p.container) for p in context.InputGroup(descs))
    iout, iout_d = context.Output(out), context.Output(out_d)
    context.ExecWithEnv(env=python, cmd=f"""
        python3 {context.Input(bsr_lib).container} merge --hits {iout.container} --descriptions {iout_d.container} \
            --hit-chunks {ihits} --description-chunks {idescs}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local, out_d: iout_d.local}],
        success=iout.local.exists() and iout_d.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=2)),
)
