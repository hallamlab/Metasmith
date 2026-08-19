from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run     = model.AddRequirement(lib.GetType("aspire::run"))
policy  = model.AddRequirement(lib.GetType("aspire::graph_network_on"), parents={run})
all     = model.AddRequirement(lib.GetType("aspire::network_graph_all"), parents={run})
thr     = model.AddRequirement(lib.GetType("aspire::network_graph_thr"), parents={run})
nf      = model.AddRequirement(lib.GetType("aspire::network_node_features"), parents={run})
counts  = model.AddRequirement(lib.GetType("aspire::analysis_counts"), parents={run})
md      = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={run})
pairing = model.AddRequirement(lib.GetType("aspire::asv_mag_pairing"), parents={run})
tax     = model.AddRequirement(lib.GetType("amplicon::asv_taxonomy"), parents={run})
tables  = model.AddRequirement(lib.GetType("aspire::indicspecies_tables"), parents={run})
sub     = model.AddRequirement(lib.GetType("aspire::network_modules_sub"), parents={run})
mall    = model.AddRequirement(lib.GetType("aspire::network_modules_all"), parents={run})
out     = model.AddProduct(lib.GetType("aspire::network_outputs"))

def protocol(context: ExecutionContext):
    made = {
        out: context.Output(out),
    }
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = {out}

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=run,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=6),
    ),
)
