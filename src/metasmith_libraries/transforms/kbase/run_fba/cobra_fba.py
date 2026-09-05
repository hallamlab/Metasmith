from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
gem     = model.AddRequirement(lib.GetType("modelling::metabolic_model"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
conds   = model.AddRequirement(lib.GetType("ecspr::conditions"))
out     = model.AddProduct(lib.GetType("modelling::fba_solution"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces: set the exchange bounds from {imedia}, then for
    # every row of {iconds} apply that row's mask to the model and maximise biomass,
    # writing one row of fluxes and one objective value per condition.
    #
    # A conditions table is one row per medium and mask, which is a KBase PhenotypeSet in
    # this repository's own idiom -- so `predict_phenotype` is absorbed here rather than
    # becoming a transform of its own.
    #
    # CAUTION round 3 declares `ecspr::conditions` OPTIONAL -- solve once given no table,
    # once per row given one. metasmith has no optional requirement: `AddRequirement` takes
    # no such flag and `Dependency` carries no such field. It is declared mandatory here
    # and a template supplies it as a deferred input. If that turns out to be the wrong
    # trade, the fix is two transforms, not a flag, and it belongs to whoever writes this
    # body.
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
    group_by=gem,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
