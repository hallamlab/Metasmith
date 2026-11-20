from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()

image  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs   = model.AddRequirement(lib.GetType("sequences::orfs"))
clean  = model.AddRequirement(lib.GetType("annotation::clean_predictions"), parents={orfs})
ev_lib = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
pt_lib = model.AddRequirement(lib.GetType("lib::pathologic"))
out    = model.AddProduct(lib.GetType("annotation::ptools_annotation_table"))


def protocol(context: ExecutionContext):
    iorfs  = context.Input(orfs)
    iclean = context.Input(clean)
    iev    = context.Input(ev_lib)
    ipt    = context.Input(pt_lib)
    iout   = context.Output(out)

    context.ExecWithEnv(env=image, cmd=f"""
        PYTHONPATH=$(dirname {iev.container}) python3 {ipt.container}/gather.py \
            --orfs {iorfs.container} --clean {iclean.container} --out {iout.container}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(minutes=30),
    ),
)
