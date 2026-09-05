from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::cobra.env"))
acc     = model.AddRequirement(lib.GetType("modelling::bigg_model_id"))
out     = model.AddProduct(lib.GetType("modelling::metabolic_model"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces:
    #   id=$(cat {iacc.container})
    #   curl -fsSL http://bigg.ucsd.edu/static/models/${id}.json -o model.json
    #   python -c "import cobra; cobra.io.write_sbml_model(cobra.io.load_json_model('model.json'), '{iout.container}')"
    #
    # The id is the accession, the way `ncbi::assembly_accession` is, so this matches
    # `logistics/getNcbiAssembly.py` in shape. A curated biomass equation and curated GPR
    # rules are the sound baseline to measure a draft reconstruction against;
    # `fabfos/build_references/vs_gem/bridge.py` holds the per-host set this generalises.
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
    group_by=acc,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(minutes=30),
    ),
)
