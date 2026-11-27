# graph_network_absent -- the `off` arm of graph_network: an empty directory, which the master
# summary scans and finds nothing in.

from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
survey = model.AddRequirement(lib.GetType("amplicon::survey"))
policy = model.AddRequirement(lib.GetType("aspire::graph_network_off"), parents={survey})
out    = model.AddProduct(lib.GetType("aspire::network_outputs"))


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    iout.local.mkdir(parents=True, exist_ok=True)
    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.is_dir())


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
