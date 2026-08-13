from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep     = model.AddRequirement(lib.GetType("mock::x"))
out     = model.AddProduct(lib.GetType("mock::target"))

def protocol(context: ExecutionContext):
    import time
    out_path = Path("output.txt")
    in_path = context.Input(dep)
    with open(in_path.local) as f:
        dt = f.readline()
        if dt.endswith("\t"): dt = dt[:-1]
        print(dt)
    time.sleep(int(dt)) # work
    context.external_shell.Exec(f"touch {out_path}")
    return ExecutionResult(
        manifest=[
            {
                out: out_path
            },
        ],
        success=out_path.exists()
    )

TransformInstance(
    protocol = protocol,
    model = model,
    group_by=dep,
    output_signature = {
        out: "output.txt",
    },
    resources = Resources(
        cpus = 4,
        memory = Size.GB(8.1),
        duration = Duration(hours=1, minutes=30),
    )
)
