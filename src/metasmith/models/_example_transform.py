from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
def protocol(context: ExecutionContext):
    context.external_shell.Exec(f"touch output.txt")
    return ExecutionResult(success=True)

model = Transform()
dep = model.AddRequirement(node=lib.GetType("transforms::example_input"))

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        model.AddProduct(lib.GetType("transforms::example_output")): "output.txt",
    },
)
