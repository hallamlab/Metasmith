from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
clust   = model.AddRequirement(lib.GetType("lib::hierarchical_clustering.py"))
local   = model.AddRequirement(lib.GetType("lib::local"))
counts  = model.AddRequirement(lib.GetType("transcriptomics::gene_count_table"))
out     = model.AddProduct(lib.GetType("transcriptomics::expression_clusters"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces: correlate genes across the columns of {icounts} and
    # cut the resulting dendrogram, using the `hierarchical_clustering.py` resource the
    # pangenome heatmap already ships. Genes that move together are the operon and the
    # regulon, which per-gene testing reports as unrelated hits.
    #
    # KBase's four apps here are hierarchical, k-means, WGCNA and an estimate of k. That is
    # one transform with a method knob, not four -- so whoever writes this body adds the
    # knob as a requirement rather than adding three more files.
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
    group_by=counts,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=1),
    ),
)
