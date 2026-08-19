from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run    = model.AddRequirement(lib.GetType("aspire::run"))
policy = model.AddRequirement(lib.GetType("aspire::spieceasi_off"), parents={run})
x_all  = model.AddRequirement(lib.GetType("aspire::external_graph_all"))
x_thr  = model.AddRequirement(lib.GetType("aspire::external_graph_thr"))
x_nf   = model.AddRequirement(lib.GetType("aspire::external_node_features"))
all    = model.AddProduct(lib.GetType("aspire::network_graph_all"))
thr    = model.AddProduct(lib.GetType("aspire::network_graph_thr"))
nf     = model.AddProduct(lib.GetType("aspire::network_node_features"))

def protocol(context: ExecutionContext):
    made = {
        all: context.Output(all),
        thr: context.Output(thr),
        nf: context.Output(nf),
    }
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = set()

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=run,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
