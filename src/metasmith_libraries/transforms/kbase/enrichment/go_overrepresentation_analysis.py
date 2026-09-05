from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
genes   = model.AddRequirement(lib.GetType("annotation::gene_set"))
ipr     = model.AddRequirement(lib.GetType("annotation::interproscan_results"))
out     = model.AddProduct(lib.GetType("annotation::go_overrepresentation"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces: a hypergeometric test of each GO term's frequency
    # in {igenes} against its frequency in {iipr}, which is the annotated genome and
    # therefore the background, with Benjamini-Hochberg over the terms tested.
    # interproscan already emits the GO terms; no new tool and no GO database download.
    #
    # The gene set is whatever selected the genes -- a DE result, a bin, a blast hit list.
    # It is deliberately NOT typed to one of those.
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
    group_by=genes,
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
