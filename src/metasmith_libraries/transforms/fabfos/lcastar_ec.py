from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"))
gpr     = model.AddRequirement(lib.GetType("annotation::gpr_table"), parents={orfs})
bridge  = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
lca_lib = model.AddRequirement(lib.GetType("lib::lcastar"))
out     = model.AddProduct(lib.GetType("annotation::ec_consensus"))


def protocol(context: ExecutionContext):
    igpr = context.Input(gpr)
    ibr = context.Input(bridge)
    iev = context.Input(ev_lib)
    ilib = context.Input(lca_lib)
    iout = context.Output(out)

    context.ExecWithEnv(env=image, cmd=f"""
        PYTHONPATH={ilib.container}:$(dirname {iev.container}) python3 {ilib.container}/ec_consensus.py \
            --gpr {igpr.container} --bridge {ibr.container} --out {iout.container}
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
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)
