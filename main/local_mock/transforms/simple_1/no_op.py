from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep     = model.AddRequirement(lib.GetType("mock::a"))
out     = model.AddProduct(lib.GetType("mock::b"))

def protocol(context: ExecutionContext):
    if context.params.get("attempt", -1)<2:
        assert False, "fail for testing retry"
    import time
    out_path = context.Get(out)
    in_path = context.Get(dep)

    if "1" in in_path.local.name:
        assert False, "fail for testing ignore"

    with open(in_path.local) as f:
        dt = f.readline()
        if dt.endswith("\t"): dt = dt[:-1]
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
