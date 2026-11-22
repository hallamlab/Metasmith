from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image   = model.AddRequirement(lib.GetType("env::signalp6.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"))
results = model.AddProduct(lib.GetType("annotation::signalp6_results"))


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iout  = context.Output(results)

    context.ExecWithEnv(env=image, cmd=f"""
        sed '/^[^>]/s/\\*//g' {iorfs.container} > clean.faa
        signalp6 --fastafile clean.faa --organism other --output_dir signalp --format none --mode fast
    """)
    context.LocalShell(f"cp signalp/prediction_results.txt {iout.local}")

    return ExecutionResult(
        manifest=[{results: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=12),
    ),
)
