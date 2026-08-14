"""merge_busco_full_table — gather per-chunk BUSCO full_table.tsv files.

BUSCO full_table.tsv starts with several `#`-prefix metadata lines (BUSCO
version, lineage dataset, etc.) ending with the `# Busco id	Status	...`
column header. From chunk 0 we keep the full `#` block; from later chunks
we strip all leading `#` lines.
"""
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_out   = model.AddRequirement(lib.GetType("annotation::busco_full_table_chunk"), parents={parent_orfs})
merged      = model.AddProduct(lib.GetType("annotation::busco_full_table"))


def protocol(context: ExecutionContext):
    import os
    chunks = sorted(context.InputGroup(chunk_out), key=lambda p: str(p.local))
    iout = context.Output(merged)

    with open(iout.local, "w") as fout:
        for i, cf in enumerate(chunks):
            with open(cf.local) as fin:
                in_header = True
                for line in fin:
                    if in_header and line.startswith("#"):
                        if i == 0:
                            fout.write(line)
                        continue
                    in_header = False
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
