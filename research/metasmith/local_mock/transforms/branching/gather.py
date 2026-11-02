from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
start    = model.AddRequirement(lib.GetType("mock::start"))
dep     = model.AddRequirement(lib.GetType("mock::x"))
out     = model.AddProduct(lib.GetType("mock::target"))

def protocol(context: ExecutionContext):
    dep_path = context.Input(dep)
    out_path = context.Output(out)
    context.external_shell.Exec(f"touch {out_path.external}")
    return ExecutionResult(
        manifest=[
            {
                out: out_path.local,
            },
        ],
        success=out_path.local.exists()
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=start,
)
