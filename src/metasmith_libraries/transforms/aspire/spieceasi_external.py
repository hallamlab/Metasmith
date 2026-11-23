# spieceasi_external -- the `off` arm of spieceasi. asv_pipeline.nf:2708-2715 substitutes
# pre-computed graph files from disk, so the driver supplies them and this passes them on.

from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
survey = model.AddRequirement(lib.GetType("amplicon::survey"))
policy = model.AddRequirement(lib.GetType("aspire::spieceasi_off"), parents={survey})
x_all  = model.AddRequirement(lib.GetType("aspire::external_graph_all"))
x_thr  = model.AddRequirement(lib.GetType("aspire::external_graph_thr"))
x_nf   = model.AddRequirement(lib.GetType("aspire::external_node_features"))
all    = model.AddProduct(lib.GetType("aspire::network_graph_all"))
thr    = model.AddProduct(lib.GetType("aspire::network_graph_thr"))
nf     = model.AddProduct(lib.GetType("aspire::network_node_features"))


def protocol(context: ExecutionContext):
    pairs = {all: x_all, thr: x_thr, nf: x_nf}
    outs = {p: context.Output(p) for p in pairs}
    for p, src in pairs.items():
        context.external_shell.Exec(f"cp {context.Input(src).external} {outs[p].external}")
    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=not [o for o in outs.values() if not o.local.exists()],
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=survey,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
