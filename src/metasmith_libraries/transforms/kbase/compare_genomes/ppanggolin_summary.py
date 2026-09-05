from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::polars.env"))
matrix  = model.AddRequirement(lib.GetType("pangenome::ppanggolin_matrix"))
kofam   = model.AddRequirement(lib.GetType("annotation::kofamscan_descriptions"))
out     = model.AddProduct(lib.GetType("pangenome::ppanggolin_summary"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces: partition the ppanggolin matrix's families into
    # core / accessory / unique by presence count, join each family's representative to
    # {ikofam} for a function, and write one row per family. A readout on an existing
    # product -- no new tool. The accessory fraction is where a lifestyle difference
    # between strains shows.
    made = {out: context.Output(out)}
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
    group_by=matrix,
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
