from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs = model.AddRequirement(lib.GetType("sequences::orfs"))
tox_chunk   = model.AddRequirement(lib.GetType("annotation::pathofact_tox_ml_chunk"), parents={parent_orfs})
vf_chunk    = model.AddRequirement(lib.GetType("annotation::pathofact_vf_ml_chunk"), parents={parent_orfs})
tox         = model.AddProduct(lib.GetType("annotation::pathofact_tox_ml"))
vf          = model.AddProduct(lib.GetType("annotation::pathofact_vf_ml"))


def _merge(chunks, out):
    with open(out, "w") as fout:
        for i, cf in enumerate(chunks):
            with open(cf.local) as fin:
                header = fin.readline()
                if i == 0:
                    fout.write(header)
                fout.writelines(fin)


def protocol(context: ExecutionContext):
    import os
    order = lambda slot: sorted(context.InputGroup(slot), key=lambda p: str(p.local))
    itox, ivf = context.Output(tox), context.Output(vf)
    _merge(order(tox_chunk), itox.local)
    _merge(order(vf_chunk), ivf.local)
    for cf in order(tox_chunk) + order(vf_chunk):
        try:
            os.unlink(cf.local)
        except OSError:
            pass

    return ExecutionResult(
        manifest=[{tox: itox.local, vf: ivf.local}],
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
