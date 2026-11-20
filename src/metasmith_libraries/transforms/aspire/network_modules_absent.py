# network_modules_absent -- the `off` arm of network_modules. asv_pipeline.nf:2726-2729 falls
# back to a zero-row placeholder; this writes the two module tables with network_modules.R's
# own empty-table columns, which graph_network reads as "no modules".

from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
survey = model.AddRequirement(lib.GetType("amplicon::survey"))
policy = model.AddRequirement(lib.GetType("aspire::network_modules_off"), parents={survey})
sub    = model.AddProduct(lib.GetType("aspire::network_modules_sub"))
mall   = model.AddProduct(lib.GetType("aspire::network_modules_all"))

HEADER = "Taxon\tmodule_id\tmodule_label\tnode_stability\tgraph_variant\tmethod\n"


def protocol(context: ExecutionContext):
    outs = {p: context.Output(p) for p in (sub, mall)}
    for o in outs.values():
        o.local.write_text(HEADER)
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
