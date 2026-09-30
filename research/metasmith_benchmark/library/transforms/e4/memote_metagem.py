from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e4::memote_0913.env"))
gem     = model.AddRequirement(lib.GetType("modelling::carveme_model_cplex"))
report  = model.AddProduct(lib.GetType("modelling::memote_report"))
results = model.AddProduct(lib.GetType("modelling::memote_results"))

SKIPS = " ".join(f"--skip {t}" for t in (
    "test_find_metabolites_produced_with_closed_bounds",
    "test_find_metabolites_consumed_with_closed_bounds",
    "test_find_metabolites_not_produced_with_open_bounds",
    "test_find_metabolites_not_consumed_with_open_bounds",
    "test_find_incorrect_thermodynamic_reversibility",
))


def protocol(context: ExecutionContext):
    igem     = context.Input(gem)
    ireport  = context.Output(report)
    iresults = context.Output(results)

    # metaGEM's memote rule. HOME moves because cobrapy creates a cache under it on import.
    context.ExecWithEnv(env=image, cmd=f"""
        export HOME="$PWD"
        cp {igem.container} gem.xml
        memote report snapshot {SKIPS} --filename {ireport.container} gem.xml
        memote run {SKIPS} gem.xml
        mv result.json.gz {iresults.container}
    """)

    return ExecutionResult(
        manifest=[{report: ireport.local, results: iresults.local}],
        success=ireport.local.exists() and iresults.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=gem,
    resources=Resources(cpus=4, memory=Size.GB(8), duration=Duration(hours=2)),
)
