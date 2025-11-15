from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep     = model.AddRequirement(lib.GetType("mock::a"))
out     = model.AddProduct(lib.GetType("mock::b"))

def protocol(context: ExecutionContext):
    import time
    out_path = context.Get(out)
    in_path = context.Get(dep)
    with open(in_path.local) as f:
        dt = f.readline()[:-1]
        print(dt)
    time.sleep(int(dt)) # work
    context.external_shell.Exec(f"touch {out_path.external}")
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "output.txt",
    },
)
