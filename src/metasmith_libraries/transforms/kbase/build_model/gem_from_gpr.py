from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
gpr     = model.AddRequirement(lib.GetType("annotation::gpr_table"))
bridge  = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
vocab   = model.AddRequirement(lib.GetType("ref::metabolism_vocab"))
out     = model.AddProduct(lib.GetType("modelling::metabolic_model"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces: resolve every reaction the GPR table names through
    # {ibridge} into a MetaNetX equation, parse it into a cobra reaction, and write the
    # result as SBML. `fabfos/build_references/vs_gem/fba_scaffold.py` already carries
    # `parse_mnx_equation` and `load_reac_prop`, which is what this reuses rather than
    # standing up a ModelSEED reconstruction lane beside them.
    #
    # CAUTION `kbase/build_model/` has TWO producers of `modelling::metabolic_model`, this
    # one and `fetch_bigg_model.py`, deliberately -- a draft and its curated baseline. A
    # target downstream of the type is therefore ambiguous and must be pinned to one.
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
    group_by=gpr,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)
