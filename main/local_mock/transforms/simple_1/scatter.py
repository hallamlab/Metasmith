from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep     = model.AddRequirement(lib.GetType("transforms::example_input"))
out     = model.AddProduct(lib.GetType("transforms::example_output"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    context.external_shell.Exec(f"touch {out_path.external}")
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "output.txt",
    },
)
