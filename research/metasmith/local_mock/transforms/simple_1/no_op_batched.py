from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
depb    = model.AddRequirement(lib.GetType("mock::b"))
dep     = model.AddRequirement(lib.GetType("mock::scattered"), parents={depb})
out     = model.AddProduct(lib.GetType("mock::target"))

def protocol(context: ExecutionContext):
    import time
    outputs = []
    for b, item_context in enumerate(context.AsBatch()):
        in_paths = item_context.InputGroup(depb)
        with open(in_paths[0].local) as f:
            dt = f.readline()
            if dt.endswith("\t"): dt = dt[:-1]
            print(dt)
        if b==0: time.sleep(int(dt))
        g = []
        for i, p in enumerate(in_paths):
            out_path = item_context.Output(out, i=i, batch=b)
            item_context.external_shell.Exec(f"touch {out_path.external}")
            g.append(out_path.local)
        outputs.append(g)
    return [
        ExecutionResult(
            manifest=[
                {
                    out: p
                }
                for p in g
            ],
        )
        for g in outputs
    ]


TransformInstance(
    protocol = protocol,
    group_by=depb,
    model = model,
    batch_size = 4,
    output_signature = {
        out: "batched.txt",
    },
)
