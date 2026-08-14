"""merge_eggnog_mapper — gather per-chunk eggNOG-mapper .annotations files.

eggNOG-mapper emits `results.emapper.annotations` with a banner of `##`-prefix
comment lines at the top, then the `#query	...` column-header line, then
data rows, then a final `##`-prefix footer. Strategy: from the first chunk,
keep everything up through the column-header line; from subsequent chunks,
strip ALL leading `#`-prefix lines; drop trailing `##` footer lines from
intermediate chunks (keep only from the last chunk).
"""
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
            # Split header (#-prefix lines at top), body, footer (trailing ##)
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
