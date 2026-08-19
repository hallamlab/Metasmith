from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_out   = model.AddRequirement(lib.GetType("annotation::eggnog_results_chunk"), parents={parent_orfs})
merged      = model.AddProduct(lib.GetType("annotation::eggnog_results"))


def protocol(context: ExecutionContext):
    import os
    chunks = sorted(context.InputGroup(chunk_out), key=lambda p: str(p.local))
    iout = context.Output(merged)
    last = len(chunks) - 1

    with open(iout.local, "w") as fout:
        for i, cf in enumerate(chunks):
            with open(cf.local) as fin:
                lines = fin.readlines()
            head_end = 0
            while head_end < len(lines) and lines[head_end].startswith("#"):
                head_end += 1
            tail_start = len(lines)
            while tail_start > head_end and lines[tail_start - 1].startswith("##"):
                tail_start -= 1
            if i == 0:
                fout.writelines(lines[:head_end])
            fout.writelines(lines[head_end:tail_start])
            if i == last:
                fout.writelines(lines[tail_start:])

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
