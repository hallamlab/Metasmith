import glob
import os
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
dep     = model.AddRequirement(lib.GetType("ncbi::accession_list"))
acc     = model.AddProduct(lib.GetType("ncbi::accession"))

def protocol(context: ExecutionContext):
    dep_path=context.Input(dep)
    outputs = []
    with open(dep_path.local) as f:
        for i, line in enumerate(f):
            outf = context.Output(acc, i=i)
            with open(outf.local, "w") as o:
                o.write(line)
            outputs.append({
                acc: outf.local,
            })
    return ExecutionResult(
        manifest=outputs,
        success=len(outputs)>0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dep,
    resources=Resources(
        cpus=1,
        memory=Size.GB(1),
        duration=Duration(hours=1),
    )
)
