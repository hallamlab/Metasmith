from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
draft   = model.AddRequirement(lib.GetType("modelling::metabolic_model"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
bridge  = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
out     = model.AddProduct(lib.GetType("modelling::gapfilled_model"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces: set the exchange bounds from {imedia}, then add the
    # smallest set of MetaNetX universe reactions that makes biomass feasible, labelling
    # every reaction added so a later reader can tell a gapfill from an annotation.
    # `fba_scaffold.build_scaffold` already inserts non-native reactions from that universe
    # into a cobra model, creating the metabolites they need and guarding the unmappable
    # ones -- this is that function with the medium as the feasibility target rather than
    # a benchmark panel.
    #
    # Without it a missing annotation returns zero growth, which reads as a biological
    # result.
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
    group_by=draft,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
