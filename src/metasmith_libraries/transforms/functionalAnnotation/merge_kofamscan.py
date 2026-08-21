from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_out   = model.AddRequirement(lib.GetType("annotation::kofamscan_results_chunk"), parents={parent_orfs})
chunk_desc  = model.AddRequirement(lib.GetType("annotation::kofamscan_descriptions_chunk"), parents={parent_orfs})
merged      = model.AddProduct(lib.GetType("annotation::kofamscan_results"))
merged_desc = model.AddProduct(lib.GetType("annotation::kofamscan_descriptions"))


def _concat_with_header(chunk_paths, out_path):
    if not chunk_paths:
        open(out_path, "w").close()
        return
    with open(chunk_paths[0].local) as fin:
        header = fin.readline()
    with open(out_path, "w") as fout:
        if header:
            fout.write(header)
        for cf in chunk_paths:
            with open(cf.local) as fin:
                for line in fin:
                    if line == header:
                        continue
                    fout.write(line)


# One row per distinct key across every chunk: the chunks are per-ORF slices of
# one search, so the same accession is described identically in as many chunks
# as hit it.
def _concat_unique(chunk_paths, out_path, sep):
    if not chunk_paths:
        open(out_path, "w").close()
        return
    with open(chunk_paths[0].local) as fin:
        header = fin.readline()
    seen = set()
    with open(out_path, "w") as fout:
        if header:
            fout.write(header)
        for cf in chunk_paths:
            with open(cf.local) as fin:
                for line in fin:
                    if line == header:
                        continue
                    key = line.split(sep, 1)[0]
                    if key in seen:
                        continue
                    seen.add(key)
                    fout.write(line)


def protocol(context: ExecutionContext):
    import os
    chunks = sorted(context.InputGroup(chunk_out), key=lambda p: str(p.local))
    descs = sorted(context.InputGroup(chunk_desc), key=lambda p: str(p.local))
    iout = context.Output(merged)
    idesc = context.Output(merged_desc)

    _concat_with_header(chunks, iout.local)
    _concat_unique(descs, idesc.local, ",")

    for cf in chunks + descs:
        try:
            os.unlink(cf.local)
        except OSError:
            pass

    return ExecutionResult(
        manifest=[{merged: iout.local, merged_desc: idesc.local}],
        success=iout.local.exists() and idesc.local.exists(),
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
