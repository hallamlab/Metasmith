from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
dep     = model.AddRequirement(lib.GetType("transforms::example_input"))
out     = model.AddProduct(lib.GetType("transforms::example_output"))

def protocol(context: ExecutionContext):
    dep_path=context.Input(dep)
    out_path=context.Output(out)
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
    group_by=dep,
    output_signature={
        out: "output.txt",
    },
)
