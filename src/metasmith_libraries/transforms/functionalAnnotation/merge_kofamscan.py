"""merge_kofamscan — gather per-chunk KofamScan CSVs.

kofamscan.py emits `gene_name,KO,thrshld,score,E-value,best` as the single
header line.
"""
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_out   = model.AddRequirement(lib.GetType("annotation::kofamscan_results_chunk"), parents={parent_orfs})
merged      = model.AddProduct(lib.GetType("annotation::kofamscan_results"))


def protocol(context: ExecutionContext):
    import os
    chunks = sorted(context.InputGroup(chunk_out), key=lambda p: str(p.local))
    iout = context.Output(merged)

    header = None
    with open(chunks[0].local) as fin:
        header = fin.readline()

    with open(iout.local, "w") as fout:
        if header:
            fout.write(header)
        for cf in chunks:
            with open(cf.local) as fin:
                for line in fin:
                    if line == header:
                        continue
                    fout.write(line)

    for cf in chunks:
        try:
            os.unlink(cf.local)
        except OSError:
            pass

    return ExecutionResult(
        manifest=[{merged: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=parent_orfs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
