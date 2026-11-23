from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs = model.AddRequirement(lib.GetType("sequences::orfs"))
tox_chunk   = model.AddRequirement(lib.GetType("annotation::pathofact_tox_hmm_chunk"), parents={parent_orfs})
vf_chunk    = model.AddRequirement(lib.GetType("annotation::pathofact_vf_hmm_chunk"), parents={parent_orfs})
db          = model.AddRequirement(lib.GetType("ref::pathofact_db"))
tox         = model.AddProduct(lib.GetType("annotation::pathofact_tox_hmm"))
vf          = model.AddProduct(lib.GetType("annotation::pathofact_vf_hmm"))
described   = model.AddProduct(lib.GetType("annotation::pathofact_hmm_descriptions"))


def _merge(chunks, out):
    hit = set()
    with open(out, "w") as fout:
        for cf in chunks:
            for line in open(cf.local):
                fout.write(line)
                if not line.startswith("#"):
                    hit.add(line.split()[2])
    return hit


def protocol(context: ExecutionContext):
    import os
    order = lambda slot: sorted(context.InputGroup(slot), key=lambda p: str(p.local))
    itox, ivf, idesc = context.Output(tox), context.Output(vf), context.Output(described)
    hit = _merge(order(tox_chunk), itox.local) | _merge(order(vf_chunk), ivf.local)

    # cddid_all.tbl: PSSM id, accession, short name, description, length. The HMMs are
    # named by accession (cd01840, pfam00004, COG0001).
    with open(context.Input(db).local/"cddid_all.tbl", errors="replace") as fin, \
            open(idesc.local, "w") as fout:
        fout.write("term\tdescription\n")
        for line in fin:
            f = line.rstrip("\n").split("\t")
            if len(f) > 3 and f[1] in hit:
                fout.write(f"{f[1]}\t{f[2]}: {f[3]}\n")

    for cf in order(tox_chunk) + order(vf_chunk):
        try:
            os.unlink(cf.local)
        except OSError:
            pass

    return ExecutionResult(
        manifest=[{tox: itox.local, vf: ivf.local, described: idesc.local}],
        success=itox.local.exists() and ivf.local.exists(),
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
