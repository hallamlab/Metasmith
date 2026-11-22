from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

python  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
report  = model.AddRequirement(lib.GetType("annotation::pathofact_report"), parents={asm})
signalp = model.AddRequirement(lib.GetType("annotation::signalp6_results"), parents={orfs})
gpr_lib = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
out     = model.AddProduct(lib.GetType("annotation::pathofact_report_signalp"))


# PathoFact's SignalP is optional and licensed, so it is a second step over the finished
# report rather than a requirement of it. SignalP 6 with PathoFact's own flags
# (--organism other --format none --mode fast) is what the signalp6 lane runs.
def protocol(context: ExecutionContext):
    iout = context.Output(out)
    context.ExecWithEnv(env=python, cmd=f"""
        python3 {context.Input(gpr_lib).container}/pathofact_report.py \\
            --report {context.Input(report).container} \\
            --signalp {context.Input(signalp).container} --out {iout.container}
    """)
    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
