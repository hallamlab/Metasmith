from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::polars.env"))
survey  = model.AddRequirement(lib.GetType("amplicon::survey"))
report  = model.AddRequirement(lib.GetType("taxonomy::kraken2_report"), parents={survey})
counts  = model.AddProduct(lib.GetType("amplicon::asv_table"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces: read every sample's kraken2 report, keep the rows
    # at the requested rank, and pivot to one sample-by-taxon count matrix.
    #
    # A kraken2 report and an ASV table are the same shape -- samples by taxa, counts --
    # so this produces `amplicon::asv_table` rather than a new type, and the six ecology
    # transforms lifted out of the aspire gate read it unchanged.
    #
    # `amplicon::survey` is the grouping node: a count matrix is one object over MANY
    # samples, and metasmith expresses that with a logistics type the per-sample inputs
    # descend from, the way `transcriptomics::experiment` and `pangenome::pangenome` do.
    made = {counts: context.Output(counts)}
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
    group_by=survey,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=1),
    ),
)
