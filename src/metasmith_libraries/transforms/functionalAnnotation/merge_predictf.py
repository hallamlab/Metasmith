"""merge_predictf — gather per-chunk PredicTF outputs (two products: TF + potential).

Both `*.mapping.TF` and `*.mapping.potential.TF` are TSV-like tables. We
treat them as one-line-header tables and concat.
"""
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs   = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_tf      = model.AddRequirement(lib.GetType("annotation::predictf_results_chunk"), parents={parent_orfs})
chunk_pot     = model.AddRequirement(lib.GetType("annotation::predictf_potential_chunk"), parents={parent_orfs})
merged_tf     = model.AddProduct(lib.GetType("annotation::predictf_results"))
merged_pot    = model.AddProduct(lib.GetType("annotation::predictf_potential"))


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


def protocol(context: ExecutionContext):
    import os
    tf_chunks  = sorted(context.InputGroup(chunk_tf),  key=lambda p: str(p.local))
    pot_chunks = sorted(context.InputGroup(chunk_pot), key=lambda p: str(p.local))
    otf  = context.Output(merged_tf)
    opot = context.Output(merged_pot)

    _concat_with_header(tf_chunks,  otf.local)
    _concat_with_header(pot_chunks, opot.local)

    for cf in tf_chunks + pot_chunks:
        try:
            os.unlink(cf.local)
        except OSError:
            pass

    return ExecutionResult(
        manifest=[{merged_tf: otf.local, merged_pot: opot.local}],
        success=otf.local.exists() and opot.local.exists(),
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
