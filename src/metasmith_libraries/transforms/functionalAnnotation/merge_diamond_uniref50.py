"""merge_diamond_uniref50 — gather per-chunk DIAMOND UniRef50 BLAST hits.

DIAMOND outfmt 6 is plain tabular with NO header line; concat is a raw cat.
"""
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_out   = model.AddRequirement(lib.GetType("annotation::diamond_uniref50_results_chunk"), parents={parent_orfs})
merged      = model.AddProduct(lib.GetType("annotation::diamond_uniref50_results"))


def protocol(context: ExecutionContext):
    import os
    chunks = sorted(context.InputGroup(chunk_out), key=lambda p: str(p.local))
    iout = context.Output(merged)

    with open(iout.local, "wb") as fout:
        for cf in chunks:
            with open(cf.local, "rb") as fin:
                while True:
                    buf = fin.read(1 << 20)
                    if not buf:
                        break
                    fout.write(buf)

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
