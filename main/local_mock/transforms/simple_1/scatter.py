from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep     = model.AddRequirement(lib.GetType("mock::b"))
out     = model.AddProduct(lib.GetType("mock::scattered"))

def protocol(context: ExecutionContext):
    outputs = []
    for i in range(2):
        out_path = context.Output(out, i=i)
        context.external_shell.Exec(f"touch {out_path.external}")
        outputs.append(out_path.local)
    return ExecutionResult(
        manifest=[
            {
                out: p
            }
            for p in outputs
        ],
    )

TransformInstance(
    protocol = protocol,
    model = model,
    group_by=dep,
    output_signature = {
        out: "scat.txt",
    },
)
