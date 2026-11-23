from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_out   = model.AddRequirement(lib.GetType("annotation::vogdb_hmmsearch_results_chunk"), parents={parent_orfs})
db          = model.AddRequirement(lib.GetType("ref::vogdb_hmm"))
merged      = model.AddProduct(lib.GetType("annotation::vogdb_hmmsearch_results"))
described   = model.AddProduct(lib.GetType("annotation::vogdb_descriptions"))


def protocol(context: ExecutionContext):
    import os
    chunks = sorted(context.InputGroup(chunk_out), key=lambda p: str(p.local))
    iout = context.Output(merged)
    idesc = context.Output(described)

    hit = set()
    with open(iout.local, "w") as fout:
        for cf in chunks:
            for line in open(cf.local):
                fout.write(line)
                if not line.startswith("#"):
                    hit.add(line.split()[3])
    descriptions = dict(line.rstrip("\n").split("\t", 1)
                        for line in open(context.Input(db).local/"descriptions.tsv"))
    with open(idesc.local, "w") as fh:
        fh.write("term\tdescription\n")
        for term in sorted(hit):
            fh.write(f"{term}\t{descriptions.get(term, '')}\n")

    for cf in chunks:
        try:
            os.unlink(cf.local)
        except OSError:
            pass

    return ExecutionResult(
        manifest=[{merged: iout.local, described: idesc.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=parent_orfs,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
